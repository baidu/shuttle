#include "job_tracker.h"

#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <algorithm>
#include <iterator>
#include <string>
#include <sstream>
#include <set>
#include <sys/time.h>

#include "google/protobuf/repeated_field.h"
#include "logging.h"
#include "proto/minion.pb.h"
#include "resource_manager.h"
#include "master_impl.h"
#include "common/tools_util.h"

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_int32(timeout_bound);
DECLARE_int32(replica_num);
DECLARE_int32(replica_begin);
DECLARE_int32(replica_begin_percent);
DECLARE_int32(retry_bound);
DECLARE_int32(blind_predict_num);
DECLARE_string(nexus_server_list);

namespace baidu {
namespace shuttle {

JobTracker::JobTracker(MasterImpl* master, ::baidu::galaxy::Galaxy* galaxy_sdk,
                       const JobDescriptor& job) :
                      master_(master),
                      galaxy_(galaxy_sdk),
                      state_(kPending),
                      map_allow_duplicates_(true),
                      reduce_allow_duplicates_(true),
                      map_(NULL),
                      map_manager_(NULL),
                      map_completed_(0),
                      reduce_(NULL),
                      reduce_manager_(NULL),
                      reduce_completed_(0),
                      monitor_(1),
                      blind_predict_(0) {
    job_descriptor_.CopyFrom(job);
    job_id_ = GenerateJobId();
    rpc_client_ = new RpcClient();
    std::vector<std::string> inputs;
    const ::google::protobuf::RepeatedPtrField<std::string>& input_filenames = job.inputs();
    std::copy(input_filenames.begin(), input_filenames.end(), std::back_inserter(inputs));

    // Prepare fs for output checking and temporary files recycling
    FileSystem::Param output_param;
    const DfsInfo& output_dfs = job_descriptor_.output_dfs();
    if (!output_dfs.user().empty() && !output_dfs.password().empty()) {
        output_param["user"] = output_dfs.user();
        output_param["password"] = output_dfs.password();
    }
    if (boost::starts_with(job_descriptor_.output(), "hdfs://")) {
        std::string host;
        int port;
        ParseHdfsAddress(job_descriptor_.output(), &host, &port, NULL);
        output_param["host"] = host;
        output_param["port"] = boost::lexical_cast<std::string>(port);
        job_descriptor_.mutable_output_dfs()->set_host(host);
        job_descriptor_.mutable_output_dfs()->set_port(boost::lexical_cast<std::string>(port));
    } else if (!output_dfs.host().empty() && !output_dfs.port().empty()) {
        output_param["host"] = output_dfs.host();
        output_param["port"] = output_dfs.port();
    }

    fs_ = FileSystem::CreateInfHdfs(output_param);
    if (fs_->Exist(job_descriptor_.output())) {
        LOG(INFO, "output exists, failed: %s", job_id_.c_str());
        job_descriptor_.set_map_total(0);
        job_descriptor_.set_reduce_total(0);
        state_ = kFailed;
        return;
    }

    // Build resource manager
    FileSystem::Param input_param;
    const DfsInfo& input_dfs = job_descriptor_.input_dfs();
    if(!input_dfs.user().empty() && !input_dfs.password().empty()) {
        input_param["user"] = input_dfs.user();
        input_param["password"] = input_dfs.password();
    }
    if (boost::starts_with(inputs[0], "hdfs://")) {
        std::string host;
        int port;
        ParseHdfsAddress(inputs[0], &host, &port, NULL);
        input_param["host"] = host;
        input_param["port"] = boost::lexical_cast<std::string>(port);
        job_descriptor_.mutable_input_dfs()->set_host(host);
        job_descriptor_.mutable_input_dfs()->set_port(boost::lexical_cast<std::string>(port));
    } else if (!input_dfs.host().empty() && !input_dfs.port().empty()) {
        input_param["host"] = input_dfs.host();
        input_param["port"] = input_dfs.port();
    }
    if (job_descriptor_.input_format() == kNLineInput) {
        map_manager_ = new NLineResourceManager(inputs, input_param);
    } else {
        map_manager_ = new ResourceManager(inputs, input_param);
    }
    int sum_of_map = map_manager_->SumOfItem();

    job_descriptor_.set_map_total(sum_of_map);
    if (job_descriptor_.map_total() < 1) {
        LOG(INFO, "map input may not inexist, failed: %s", job_id_.c_str());
        job_descriptor_.set_reduce_total(0);
        state_ = kFailed;
        return;
    }
    if (job.job_type() == kMapReduceJob) {
        reduce_manager_ = new IdManager(job_descriptor_.reduce_total());
    } else {
        reduce_manager_ = NULL;
    }
    failed_count_.resize(sum_of_map, 0);

    // For end game counter
    map_end_game_begin_ = sum_of_map - FLAGS_replica_begin;
    int temp = sum_of_map * FLAGS_replica_begin_percent / 100;
    if (map_end_game_begin_ < temp) {
        map_end_game_begin_ = temp;
    }
    if (reduce_manager_ == NULL) {
        return;
    }
    reduce_end_game_begin_ = reduce_manager_->SumOfItem() - FLAGS_replica_begin;
    temp = reduce_manager_->SumOfItem() * FLAGS_replica_begin_percent / 100;
    if (reduce_end_game_begin_ < temp) {
        reduce_end_game_begin_ = temp;
    }
    map_allow_duplicates_ = job_descriptor_.map_allow_duplicates();
    reduce_allow_duplicates_ = job_descriptor_.reduce_allow_duplicates();
}

JobTracker::~JobTracker() {
    Kill();
    if (map_manager_ != NULL) {
        delete map_manager_;
    }
    if (reduce_manager_ != NULL) {
        delete reduce_manager_;
    }
    delete rpc_client_;
    {
        MutexLock lock(&alloc_mu_);
        for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            delete *it;
        }
    }
    delete fs_;
}

Status JobTracker::Start() {
    if (fs_->Exist(job_descriptor_.output())) {
        return kWriteFileFail;
    }
    if (state_ == kFailed) {
        // For now the failed status means invalid input
        return kNoMore;
    }
    map_ = new Gru(galaxy_, &job_descriptor_, job_id_,
            (job_descriptor_.job_type() == kMapOnlyJob) ? kMapOnly : kMap);
    if (map_->Start() == kOk) {
        LOG(INFO, "start a new map reduce job: %s -> %s",
                job_descriptor_.name().c_str(), job_id_.c_str());
        return kOk;
    }
    LOG(WARNING, "galaxy report error when submitting a new job: %s",
            job_descriptor_.name().c_str());
    return kGalaxyError;
}

static inline JobPriority ParsePriority(const std::string& priority) {
    return priority == "kMonitor" ? kVeryHigh : (
               priority == "kOnline" ? kHigh : (
                   priority == "kOffline" ? kNormal : (
                       priority == "kBestEffort" ? kLow : kNormal
                   )
               )
           );
}

Status JobTracker::Update(const std::string& priority,
                          int map_capacity,
                          int reduce_capacity) {
    if (map_ != NULL) {
        if (map_->Update(priority, map_capacity) != kOk) {
            return kGalaxyError;
        }
        if (map_capacity != -1) {
            job_descriptor_.set_map_capacity(map_capacity);
        }
        if (!priority.empty()) {
            job_descriptor_.set_priority(ParsePriority(priority));
        }
    }

    if (reduce_ != NULL) {
        if (reduce_->Update(priority, reduce_capacity) != kOk) {
            return kGalaxyError;
        }
        if (map_capacity != -1) {
            job_descriptor_.set_reduce_capacity(reduce_capacity);
        }
        if (!priority.empty()) {
            job_descriptor_.set_priority(ParsePriority(priority));
        }
    }
    return kOk;
}

Status JobTracker::Kill() {
    {
        MutexLock lock(&mu_);
        if (map_ != NULL) {
            LOG(INFO, "map minion finished, kill: %s", job_id_.c_str());
            delete map_;
            map_ = NULL;
        }

        if (reduce_ != NULL) {
            LOG(INFO, "reduce minion finished, kill: %s", job_id_.c_str());
            delete reduce_;
            reduce_ = NULL;
        }
        monitor_.Stop(false);

        if (state_ != kCompleted) {
            state_ = kKilled;
        }
    }

    MutexLock lock(&alloc_mu_);
    for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
            it != allocation_table_.end(); ++it) {
        if ((*it)->state == kTaskRunning) {
            (*it)->state = kTaskKilled;
        }
    }
    return kOk;
}

ResourceItem* JobTracker::AssignMap(const std::string& endpoint) {
    if (state_ == kPending) {
        state_ = kRunning;
    }
    ResourceItem* cur = map_manager_->GetItem();
    if (cur == NULL) {
        if (!map_allow_duplicates_) {
            LOG(DEBUG, "assign map: no more: %s", job_id_.c_str());
            return NULL;
        }
        MutexLock lock(&alloc_mu_);
        while (!map_slug_.empty() &&
               map_manager_->CheckCertainItem(map_slug_.front())->status
               != kResAllocated) {
            map_slug_.pop();
        }
        if (map_slug_.empty()) {
            LOG(DEBUG, "assign map: no more: %s", job_id_.c_str());
            return NULL;
        }
        cur = map_manager_->GetCertainItem(map_slug_.front());
        map_slug_.pop();
        if (cur == NULL) {
            LOG(DEBUG, "assign map: no more: %s", job_id_.c_str());
            return NULL;
        }
    } else if (map_allow_duplicates_ && cur->no >= map_end_game_begin_) {
        MutexLock lock(&alloc_mu_);
        for (int i = 0; i < FLAGS_replica_num; ++i) {
            map_slug_.push(cur->no);
        }
    }
    if (map_allow_duplicates_ && cur->no == map_end_game_begin_) {
        monitor_.AddTask(boost::bind(&JobTracker::KeepMonitoring, this));
    }
    AllocateItem* alloc = new AllocateItem();
    alloc->endpoint = endpoint;
    alloc->state = kTaskRunning;
    alloc->resource_no = cur->no;
    alloc->attempt = cur->attempt;
    alloc->state = kTaskRunning;
    alloc->is_map = true;
    alloc->alloc_time = std::time(NULL);
    alloc->period = -1;
    MutexLock lock(&alloc_mu_);
    allocation_table_.push_back(alloc);
    time_heap_.push(alloc);
    LOG(INFO, "assign map: < no - %d, attempt - %d >, to %s: %s",
            alloc->resource_no, alloc->attempt, endpoint.c_str(), job_id_.c_str());
    return cur;
}

IdItem* JobTracker::AssignReduce(const std::string& endpoint) {
    if (state_ == kPending) {
        state_ = kRunning;
    }
    IdItem* cur = reduce_manager_->GetItem();
    if (cur == NULL) {
        if (!reduce_allow_duplicates_) {
            LOG(DEBUG, "assign reduce: no more: %s", job_id_.c_str());
            return NULL;
        }
        MutexLock lock(&alloc_mu_);
        while (!reduce_slug_.empty() &&
               reduce_manager_->CheckCertainItem(reduce_slug_.front())->status
               != kResAllocated) {
            reduce_slug_.pop();
        }
        if (reduce_slug_.empty()) {
            LOG(DEBUG, "assign reduce: no more: %s", job_id_.c_str());
            return NULL;
        }
        cur = reduce_manager_->GetCertainItem(reduce_slug_.front());
        reduce_slug_.pop();
        if (cur == NULL) {
            LOG(DEBUG, "assign reduce: no more: %s", job_id_.c_str());
            return NULL;
        }
    } else if (reduce_allow_duplicates_ && cur->no >= reduce_end_game_begin_) {
        MutexLock lock(&alloc_mu_);
        for (int i = 0; i < FLAGS_replica_num; ++i) {
            reduce_slug_.push(cur->no);
        }
    }
    if (reduce_allow_duplicates_ && cur->no == reduce_end_game_begin_) {
        monitor_.AddTask(boost::bind(&JobTracker::KeepMonitoring, this));
    }
    AllocateItem* alloc = new AllocateItem();
    alloc->endpoint = endpoint;
    alloc->state = kTaskRunning;
    alloc->resource_no = cur->no;
    alloc->attempt = cur->attempt;
    alloc->state = kTaskRunning;
    alloc->is_map = false;
    alloc->alloc_time = std::time(NULL);
    alloc->period = -1;
    MutexLock lock(&alloc_mu_);
    allocation_table_.push_back(alloc);
    time_heap_.push(alloc);
    LOG(INFO, "assign reduce: < no - %d, attempt - %d >, to %s: %s",
            alloc->resource_no, alloc->attempt, endpoint.c_str(), job_id_.c_str());
    return cur;
}

Status JobTracker::FinishMap(int no, int attempt, TaskState state) {
    AllocateItem* cur = NULL;
    {
        MutexLock lock(&alloc_mu_);
        std::list<AllocateItem*>::iterator it;
        for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
            if ((*it)->resource_no == no && (*it)->attempt == attempt) {
                if ((*it)->state == kTaskRunning) {
                    cur = *it;
                }
                break;
            }
        }
    }
    if (cur == NULL) {
        LOG(WARNING, "try to finish an inexist map task: < no - %d, attempt - %d >: %s",
                no, attempt, job_id_.c_str());
        return kNoMore;
    }
    LOG(INFO, "finish a map task: < no - %d, attempt - %d >, state %s: %s",
            cur->resource_no, cur->attempt, TaskState_Name(state).c_str(), job_id_.c_str());
    cur->state = state;
    {
        MutexLock lock(&mu_);
        switch (state) {
        case kTaskCompleted:
            if (!map_manager_->FinishItem(cur->resource_no)) {
                LOG(WARNING, "ignore finish map request: %s, %d",
                        job_id_.c_str(), cur->resource_no);
                state = kTaskCanceled;
                break;
            }
            map_completed_ ++;
            LOG(INFO, "complete a map task(%d/%d): %s",
                    map_completed_, map_manager_->SumOfItem(), job_id_.c_str());
            if (map_completed_ == map_end_game_begin_ && job_descriptor_.job_type() != kMapOnlyJob) {
                LOG(INFO, "map phrase nearly ends, pull up reduce tasks: %s", job_id_.c_str());
                reduce_ = new Gru(galaxy_, &job_descriptor_, job_id_, kReduce);
                if (reduce_->Start() != kOk) {
                    LOG(WARNING, "reduce failed due to galaxy issue: %s", job_id_.c_str());
                }
            }
            if (map_completed_ == map_manager_->SumOfItem()) {
                if (job_descriptor_.job_type() == kMapOnlyJob) {
                    LOG(INFO, "map-only job finish: %s", job_id_.c_str());
                    fs_->Remove(job_descriptor_.output() + "/_temporary");
                    mu_.Unlock();
                    master_->RetractJob(job_id_);
                    mu_.Lock();
                    state_ = kCompleted;
                } else {
                    LOG(INFO, "map phrase ends now: %s", job_id_.c_str());
                    failed_count_.resize(0);
                    failed_count_.resize(reduce_manager_->SumOfItem(), 0);
                    mu_.Unlock();
                    if (map_allow_duplicates_) {
                        MutexLock lock(&alloc_mu_);
                        std::vector<AllocateItem*> rest;
                        while (!time_heap_.empty()) {
                            if (!time_heap_.top()->is_map) {
                                rest.push_back(time_heap_.top());
                            }
                            time_heap_.pop();
                        }
                        for (std::vector<AllocateItem*>::iterator it = rest.begin();
                                it != rest.end(); ++it) {
                            time_heap_.push(*it);
                        }
                    }
                    mu_.Lock();
                    if (map_allow_duplicates_) {
                        monitor_.CancelTask(monitor_id_);
                    }
                    if (map_ != NULL) {
                        LOG(INFO, "map minion finished, kill: %s", job_id_.c_str());
                        delete map_;
                        map_ = NULL;
                    }
                }
            }
            break;
        case kTaskFailed:
            map_manager_->ReturnBackItem(cur->resource_no);
            failed_count_[cur->resource_no] = failed_count_[cur->resource_no] + 1;
            if (failed_count_[cur->resource_no] >= FLAGS_retry_bound) {
                LOG(INFO, "map failed, kill job: %s", job_id_.c_str());
                mu_.Unlock(); 
                master_->RetractJob(job_id_);
                mu_.Lock();
                state_ = kFailed;
            }
            break;
        case kTaskKilled: break;
        case kTaskCanceled: break; // TODO Think carefully
        default: LOG(WARNING, "unfamiliar task finish status: %d", cur->state);
        }
    }
    cur->period = std::time(NULL) - cur->alloc_time;
    if (state != kTaskCompleted) {
        return kOk;
    }
    if (!map_allow_duplicates_) {
        return kOk;
    }
    Minion_Stub* stub = NULL;
    CancelTaskRequest request;
    CancelTaskResponse response;
    request.set_job_id(job_id_);
    MutexLock lock(&alloc_mu_);
    for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
            it != allocation_table_.end(); ++it) {
        if ((*it)->resource_no == no && (*it)->attempt != attempt) {
            rpc_client_->GetStub((*it)->endpoint, &stub);
            boost::scoped_ptr<Minion_Stub> stub_guard(stub);
            request.set_task_id((*it)->resource_no);
            request.set_attempt_id((*it)->attempt);
            // TODO Maybe check returned state here?
            (*it)->state = kTaskCanceled;
            alloc_mu_.Unlock();
            LOG(INFO, "cancel task: job:%s, task:%d, attempt:%d",
                job_id_.c_str(), (*it)->resource_no, (*it)->attempt);
            bool ok = rpc_client_->SendRequest(stub, &Minion_Stub::CancelTask,
                                               &request, &response, 2, 1);
            if (!ok) {
                LOG(WARNING, "failed to rpc: %s", (*it)->endpoint.c_str());
            }
            (*it)->period = std::time(NULL) - cur->alloc_time;
            alloc_mu_.Lock();
        }
    }
    return kOk;
}

Status JobTracker::FinishReduce(int no, int attempt, TaskState state) {
    AllocateItem* cur = NULL;
    {
        MutexLock lock(&alloc_mu_);
        std::list<AllocateItem*>::iterator it;
        for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
            if (!((*it)->is_map) && (*it)->resource_no == no && (*it)->attempt == attempt) {
                if ((*it)->state == kTaskRunning) {
                    cur = *it;
                }
                break;
            }
        }
    }
    if (cur == NULL) {
        LOG(WARNING, "try to finish an inexist reduce task: < no - %d, attempt - %d >: %s",
                no, attempt, job_id_.c_str());
        return kNoMore;
    }
    LOG(INFO, "finish a reduce task: < no - %d, attempt - %d >, state %s: %s",
            cur->resource_no, cur->attempt, TaskState_Name(state).c_str(), job_id_.c_str());
    cur->state = state;
    {
        MutexLock lock(&mu_);
        switch (state) {
        case kTaskCompleted:
            if (!reduce_manager_->FinishItem(cur->resource_no)) {
                LOG(WARNING, "ignore finish reduce request: %s, %d",
                    job_id_.c_str(), cur->resource_no);
                state = kTaskCanceled;
                break;
            }
            reduce_completed_ ++;
            LOG(INFO, "complete a reduce task(%d/%d): %s",
                    reduce_completed_, reduce_manager_->SumOfItem(), job_id_.c_str());
            if (reduce_completed_ == reduce_manager_->SumOfItem()) {
                LOG(INFO, "map-reduce job finish: %s", job_id_.c_str());
                std::string work_dir = job_descriptor_.output() + "/_temporary";
                LOG(INFO, "remove temp work directory: %s", work_dir.c_str());
                if (!fs_->Remove(work_dir)) {
                    LOG(WARNING, "remove temp failed");
                }
                mu_.Unlock();
                master_->RetractJob(job_id_);
                mu_.Lock();
                state_ = kCompleted;
            }
            break;
        case kTaskFailed:
            reduce_manager_->ReturnBackItem(cur->resource_no);
            failed_count_[cur->resource_no] = failed_count_[cur->resource_no] + 1;
            if (failed_count_[cur->resource_no] >= FLAGS_retry_bound) {
                LOG(INFO, "reduce failed, kill job: %s", job_id_.c_str());
                mu_.Unlock();
                master_->RetractJob(job_id_);
                mu_.Lock();
                state_ = kFailed;
            }
            break;
            break;
        case kKilled: break;
        case kTaskCanceled: break; // TODO Think carefully
        default: LOG(WARNING, "unfamiliar task finish status: %d", cur->state);
        }
    }
    cur->period = std::time(NULL) - cur->alloc_time;
    if (state != kTaskCompleted) {
        return kOk;
    }
    if (!reduce_allow_duplicates_) {
        return kOk;
    }
    Minion_Stub* stub = NULL;
    CancelTaskRequest request;
    CancelTaskResponse response;
    request.set_job_id(job_id_);
    MutexLock lock(&alloc_mu_);
    for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
            it != allocation_table_.end(); ++it) {
        if (!((*it)->is_map) && (*it)->resource_no == no && (*it)->attempt != attempt) {
            rpc_client_->GetStub((*it)->endpoint, &stub);
            boost::scoped_ptr<Minion_Stub> stub_guard(stub);
            request.set_task_id((*it)->resource_no);
            request.set_attempt_id((*it)->attempt);
            bool ok = rpc_client_->SendRequest(stub, &Minion_Stub::CancelTask,
                                               &request, &response, 2, 1);
            if (!ok) {
                LOG(WARNING, "failed to rpc: %s", (*it)->endpoint.c_str());
            }
            // TODO Maybe check returned state here?
            (*it)->state = kTaskCanceled;
        }
    }
    return kOk;
}

TaskStatistics JobTracker::GetMapStatistics() {
    std::set<int> map_blocks;
    int running = 0, failed = 0, killed = 0;
    {
        MutexLock lock(&alloc_mu_);
        for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            AllocateItem* cur = *it;
            if (!cur->is_map) {
                continue;
            }
            switch (cur->state) {
            case kTaskRunning:
                if (map_blocks.find(cur->resource_no) != map_blocks.end()) {
                    continue;
                }
                ++ running; break;
            case kTaskFailed: ++ failed; break;
            case kTaskKilled: ++ killed; break;
            default: break;
            }
            map_blocks.insert(cur->resource_no);
        }
    }
    MutexLock lock(&mu_);
    TaskStatistics task;
    task.set_total(job_descriptor_.map_total());
    task.set_pending(task.total() - running - map_completed_);
    task.set_running(running);
    task.set_failed(failed);
    task.set_killed(killed);
    task.set_completed(map_completed_);
    return task;
}

TaskStatistics JobTracker::GetReduceStatistics() {
    std::set<int> reduce_blocks;
    int running = 0, failed = 0, killed = 0;
    {
        MutexLock lock(&alloc_mu_);
        for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            AllocateItem* cur = *it;
            if (cur->is_map) {
                continue;
            }
            switch (cur->state) {
            case kTaskRunning:
                if (reduce_blocks.find(cur->resource_no) != reduce_blocks.end()) {
                    continue;
                }
                ++ running; break;
            case kTaskFailed: ++ failed; break;
            case kTaskKilled: ++ killed; break;
            default: break;
            }
            reduce_blocks.insert(cur->resource_no);
        }
    }
    MutexLock lock(&mu_);
    TaskStatistics task;
    task.set_total(job_descriptor_.reduce_total());
    task.set_pending(task.total() - running - reduce_completed_);
    task.set_running(running);
    task.set_failed(failed);
    task.set_killed(killed);
    task.set_completed(reduce_completed_);
    return task;
}

std::string JobTracker::GenerateJobId() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    std::stringstream ss;
    ss << "job_" << (t.tm_year + 1900) << (t.tm_mon + 1) << t.tm_mday
       << t.tm_hour << t.tm_min << t.tm_sec << "_"
       << boost::lexical_cast<std::string>(random());
    return ss.str();
}

void JobTracker::KeepMonitoring() {
    // Dynamic determination of delay check
    LOG(INFO, "[monitor] monitor starts to check timeout: %s", job_id_.c_str());
    bool map_now = (map_ != NULL);
    std::vector<int> time_used;
    {
        MutexLock lock(&alloc_mu_);
        for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            if ((*it)->is_map == map_now && (*it)->state == kTaskCompleted) {
                time_used.push_back((*it)->period);
            }
        }
    }
    time_t timeout = FLAGS_timeout_bound;
    if (!time_used.empty()) {
        std::sort(time_used.begin(), time_used.end());
        timeout = time_used[time_used.size() / 2];
        timeout += timeout / 5;
        blind_predict_ = 0;
    }
    if (blind_predict_ >= FLAGS_blind_predict_num) {
        monitor_id_ = monitor_.DelayTask(timeout * 1000,
                boost::bind(&JobTracker::KeepMonitoring, this));
        return;
    }
    time_t now = std::time(NULL);
    int counter = 10;
    bool detect_timeout = false;
    alloc_mu_.Lock();
    while (-- counter >= 0 && !time_heap_.empty()) {
        AllocateItem* top = time_heap_.top();
        if (now - top->alloc_time < timeout) {
            break;
        }
        time_heap_.pop();
        if (top->state != kTaskRunning) {
            ++ counter;
            continue;
        }
        if (top->is_map) {
            map_slug_.push(top->resource_no);
        } else {
            reduce_slug_.push(top->resource_no);
        }
        detect_timeout = true;
        LOG(INFO, "Reallocate a long no-response tasks: < no - %d, attempt - %d>: %s",
                top->resource_no, top->attempt, job_id_.c_str());
    }
    alloc_mu_.Unlock();
    if (time_used.empty() && detect_timeout) {
        ++ blind_predict_;
        LOG(INFO, "[monitor] blind prediction: %d times left",
                FLAGS_blind_predict_num - blind_predict_);
    }
    monitor_id_ = monitor_.DelayTask(timeout * 1000,
            boost::bind(&JobTracker::KeepMonitoring, this));
    LOG(INFO, "[monitor] will now rest for %ds: %s", timeout, job_id_.c_str());
}

}
}

