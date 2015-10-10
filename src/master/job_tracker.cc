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

#include "google/protobuf/repeated_field.h"
#include "timer.h"
#include "logging.h"
#include "proto/minion.pb.h"
#include "resource_manager.h"
#include "master_impl.h"

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_int32(timeout_bound);
DECLARE_int32(replica_num);
DECLARE_int32(replica_begin);
DECLARE_int32(retry_bound);
DECLARE_string(nexus_server_list);

namespace baidu {
namespace shuttle {

const std::string shuttle_label = "map_reduce_shuttle";

JobTracker::JobTracker(MasterImpl* master, ::baidu::galaxy::Galaxy* galaxy_sdk,
                       const JobDescriptor& job) :
                      master_(master),
                      sdk_(galaxy_sdk),
                      last_map_no_(-1),
                      last_map_attempt_(0),
                      last_reduce_no_(-1),
                      last_reduce_attempt_(0) {
    char time_chars[32];
    ::baidu::common::timer::now_time_str(time_chars, 32);
    std::string time_str = time_chars;
    boost::replace_all(time_str, " ", "-");
    job_id_ = time_str;
    job_id_ += boost::lexical_cast<std::string>(random());
    job_descriptor_.CopyFrom(job);
    state_ = kPending;
    rpc_client_ = new RpcClient();
    std::vector<std::string> inputs;
    const ::google::protobuf::RepeatedPtrField<std::string>& input_filenames = job.inputs();
    std::copy(input_filenames.begin(), input_filenames.end(), std::back_inserter(inputs));
    map_manager_ = new ResourceManager(inputs);
    job_descriptor_.set_map_total(map_manager_->SumOfItem());
    map_stat_.set_total(map_manager_->SumOfItem());
    map_stat_.set_pending(map_stat_.total());
    map_stat_.set_running(0);
    map_stat_.set_failed(0);
    map_stat_.set_killed(0);
    map_stat_.set_completed(0);
    if (job.job_type() == kMapReduceJob) {
        reduce_stat_.set_total(job.reduce_total());
        reduce_stat_.set_pending(job.reduce_total());
        reduce_stat_.set_running(0);
        reduce_stat_.set_failed(0);
        reduce_stat_.set_killed(0);
        reduce_stat_.set_completed(0);
    }
    monitor_.AddTask(boost::bind(&JobTracker::KeepMonitoring, this));
}

JobTracker::~JobTracker() {
    Kill();
    delete map_manager_;
    delete rpc_client_;
    {
        MutexLock lock(&alloc_mu_);
        for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            delete *it;
        }
    }
}

Status JobTracker::Start() {
    ::baidu::galaxy::JobDescription galaxy_job;
    galaxy_job.job_name = job_descriptor_.name() + "@minion";
    galaxy_job.type = "kBatch";
    galaxy_job.priority = "kOnline";
    galaxy_job.replica = job_descriptor_.map_capacity();
    galaxy_job.deploy_step = FLAGS_galaxy_deploy_step;
    galaxy_job.pod.requirement.millicores = job_descriptor_.millicores();
    galaxy_job.pod.requirement.memory = job_descriptor_.memory();
    std::stringstream ss;
    ss << "app_package=" << job_descriptor_.files(0) << " ./minion_boot.sh"
       << " -jobid=" << job_id_ << " -nexus_addr=" << FLAGS_nexus_server_list
       << " -work_mode=" << ((job_descriptor_.job_type() == kMapOnlyJob) ? "map-only" : "map");
    ::baidu::galaxy::TaskDescription minion;
    minion.offset = 1;
    minion.binary = FLAGS_minion_path;
    minion.source_type = "kSourceTypeFTP";
    minion.start_cmd = ss.str().c_str();
    minion.requirement = galaxy_job.pod.requirement;
    galaxy_job.pod.tasks.push_back(minion);
    std::string minion_id;
    if (sdk_->SubmitJob(galaxy_job, &minion_id)) {
        MutexLock lock(&mu_);
        map_minion_ = minion_id;
        return kOk;
    }
    return kGalaxyError;
}

Status JobTracker::Update(const std::string& priority,
                          int map_capacity,
                          int reduce_capacity) {
    if (!map_minion_.empty()) {
        mu_.Lock();
        ::baidu::galaxy::JobDescription map_desc = map_description_;
        mu_.Unlock();
        map_desc.priority = priority;
        map_desc.replica = map_capacity;
        if (sdk_->UpdateJob(map_minion_, map_desc)) {
            MutexLock lock(&mu_);
            map_description_.priority = priority;
            map_description_.replica = map_capacity;
        } else {
            return kGalaxyError;
        }
    }

    if (!reduce_minion_.empty()) {
        mu_.Lock();
        ::baidu::galaxy::JobDescription reduce_desc = reduce_description_;
        mu_.Unlock();
        reduce_desc.priority = priority;
        reduce_desc.replica = reduce_capacity;
        if (sdk_->UpdateJob(reduce_minion_, reduce_desc)) {
            MutexLock lock(&mu_);
            reduce_description_.priority = priority;
            reduce_description_.replica = reduce_capacity;
        } else {
            return kGalaxyError;
        }
    }
    return kOk;
}

Status JobTracker::Kill() {
    MutexLock lock(&mu_);
    if (!map_minion_.empty() && !sdk_->TerminateJob(map_minion_)) {
        return kGalaxyError;
    }
    map_minion_ = "";
    if (!reduce_minion_.empty() && !sdk_->TerminateJob(reduce_minion_)) {
        return kGalaxyError;
    }
    reduce_minion_ = "";
    state_ = kKilled;
    return kOk;
}

ResourceItem* JobTracker::AssignMap(const std::string& endpoint) {
    if (state_ == kPending) {
        state_ = kRunning;
    }
    ResourceItem* cur = NULL;
    AllocateItem* alloc = new AllocateItem();
    alloc->endpoint = endpoint;
    alloc->state = kTaskRunning;
    if (last_map_no_ != -1 && map_manager_->SumOfItem() - last_map_no_ < FLAGS_replica_begin &&
            last_map_attempt_ <= FLAGS_replica_num) {
        cur = map_manager_->GetCertainItem(last_map_no_);
        if (cur == NULL) {
            delete alloc;
            return NULL;
        }
        alloc->resource_no = last_map_no_;
    } else {
        cur = map_manager_->GetItem();
        if (cur == NULL) {
            delete alloc;
            return NULL;
        }
        alloc->resource_no = cur->no;
    }
    alloc->attempt = cur->attempt;
    alloc->alloc_time = std::time(NULL);
    last_map_no_ = cur->no;
    last_map_attempt_ = cur->attempt;

    int attempt_bound = FLAGS_retry_bound;
    if (map_manager_->SumOfItem() - cur->no < FLAGS_replica_begin) {
        attempt_bound += FLAGS_replica_num;
    }
    if (cur->attempt > attempt_bound) {
        master_->RetractJob(job_id_);
        state_ = kFailed;
        delete alloc;
        return NULL;
    }
    MutexLock lock(&alloc_mu_);
    allocation_table_.push_back(alloc);
    time_heap_.push(alloc);
    return cur;
}

IdItem* JobTracker::AssignReduce(const std::string& endpoint) {
    if (state_ == kPending) {
        state_ = kRunning;
    }
    IdItem* cur = NULL;
    AllocateItem* alloc = new AllocateItem();
    alloc->endpoint = endpoint;
    alloc->state = kTaskRunning;
    if (last_reduce_no_ != -1 && reduce_manager_->SumOfItem() - last_reduce_no_ < FLAGS_replica_begin
            && last_reduce_attempt_ <= FLAGS_replica_num) {
        cur = reduce_manager_->GetCertainItem(last_reduce_no_);
        if (cur == NULL) {
            delete alloc;
            return NULL;
        }
        alloc->resource_no = last_reduce_no_;
    } else {
        cur = reduce_manager_->GetItem();
        if (cur == NULL) {
            delete alloc;
            return NULL;
        }
        alloc->resource_no = cur->no;
    }
    alloc->attempt = cur->attempt;
    alloc->alloc_time = std::time(NULL);
    last_reduce_no_ = cur->no;
    last_reduce_attempt_ = cur->attempt;

    int attempt_bound = FLAGS_retry_bound;
    if (reduce_manager_->SumOfItem() - cur->no < FLAGS_replica_begin) {
        attempt_bound += FLAGS_replica_num;
    }
    if (cur->attempt > attempt_bound) {
        master_->RetractJob(job_id_);
        state_ = kFailed;
        delete alloc;
        return NULL;
    }
    MutexLock lock(&alloc_mu_);
    allocation_table_.push_back(alloc);
    time_heap_.push(alloc);
    return cur;
}

Status JobTracker::FinishMap(int no, int attempt, TaskState state) {
    AllocateItem* cur = NULL;
    {
        MutexLock lock(&alloc_mu_);
        std::list<AllocateItem*>::iterator it;
        for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
            if ((*it)->resource_no == no && (*it)->attempt == attempt) {
                cur = *it;
            }
        }
    }
    if (cur == NULL) {
        LOG(WARNING, "try to finish an inexist task: < no - %d, attempt - %d >", no, attempt);
        return kNoMore;
    }
    cur->state = state;
    {
        MutexLock lock(&mu_);
        switch (state) {
        case kTaskCompleted:
            map_stat_.set_completed(map_stat_.completed() + 1);
            if (map_stat_.completed() == map_manager_->SumOfItem()) {
                master_->RetractJob(job_id_);
                state_ = kCompleted;
            }
            break;
        case kTaskKilled: map_stat_.set_killed(map_stat_.killed() + 1); break;
        case kTaskFailed:
            map_stat_.set_failed(map_stat_.failed() + 1);
            map_manager_->ReturnBackItem(cur->resource_no);
            break;
        case kTaskCanceled: break; // TODO Think carefully
        default: LOG(WARNING, "unfamiliar task finish status: %d", cur->state);
        }
    }
    if (state != kTaskCompleted) {
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
            bool ok = rpc_client_->SendRequest(stub, &Minion_Stub::CancelTask,
                                               &request, &response, 2, 1);
            if (!ok) {
                LOG(WARNING, "failed to rpc: %s", (*it)->endpoint.c_str());
            }
            // TODO Maybe check returned state here?
        }
    }
    return kOk;
}

Status JobTracker::FinishReduce(int no, int attempt, TaskState state) {
    AllocateItem* cur = NULL;
    {
        MutexLock lock(&alloc_mu_);
        // TODO XXX
        // Warning:
        //     This table is not supposed to be public for map and reduce.
        //     So it won't work until proper modification
        std::list<AllocateItem*>::iterator it;
        for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
            if ((*it)->resource_no == no && (*it)->attempt == attempt) {
                cur = *it;
            }
        }
    }
    if (cur == NULL) {
        LOG(WARNING, "try to finish an inexist task: < no - %d, attempt - %d >", no, attempt);
        return kNoMore;
    }
    cur->state = state;
    {
        MutexLock lock(&mu_);
        switch (state) {
        case kTaskCompleted:
            reduce_stat_.set_completed(reduce_stat_.completed() + 1);
            if (reduce_stat_.completed() == reduce_manager_->SumOfItem()) {
                master_->RetractJob(job_id_);
                state_ = kCompleted;
            }
            break;
        case kTaskKilled: reduce_stat_.set_killed(reduce_stat_.killed() + 1); break;
        case kTaskFailed:
            reduce_stat_.set_failed(reduce_stat_.failed() + 1);
            reduce_manager_->ReturnBackItem(cur->resource_no);
            break;
        case kTaskCanceled: break; // TODO Think carefully
        default: LOG(WARNING, "unfamiliar task finish status: %d", cur->state);
        }
    }
    if (state != kTaskCompleted) {
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
            bool ok = rpc_client_->SendRequest(stub, &Minion_Stub::CancelTask,
                                               &request, &response, 2, 1);
            if (!ok) {
                LOG(WARNING, "failed to rpc: %s", (*it)->endpoint.c_str());
            }
            // TODO Maybe check returned state here?
        }
    }
    return kOk;
}

void JobTracker::KeepMonitoring() {
    time_t now = std::time(NULL);
    while (!time_heap_.empty()) {
        alloc_mu_.Lock();
        AllocateItem* top = time_heap_.top();
        if (now - top->alloc_time < FLAGS_timeout_bound) {
            alloc_mu_.Unlock();
            break;
        }
        time_heap_.pop();
        alloc_mu_.Unlock();
        if (top->state != kTaskRunning) {
            delete top;
            continue;
        }
        map_manager_->ReturnBackItem(top->resource_no);
        Minion_Stub* stub = NULL;
        rpc_client_->GetStub(top->endpoint, &stub);
        boost::scoped_ptr<Minion_Stub> stub_guard(stub);
        CancelTaskRequest request;
        CancelTaskResponse response;
        request.set_job_id(job_id_);
        request.set_task_id(top->resource_no);
        request.set_attempt_id(top->attempt);
        bool ok = rpc_client_->SendRequest(stub, &Minion_Stub::CancelTask,
                                           &request, &response, 2, 1);
        if (!ok) {
            LOG(WARNING, "failed to rpc: %s", top->endpoint.c_str());
        }
        // TODO Maybe check returned state here?
        map_stat_.set_killed(map_stat_.killed());

        delete top;
    }
    monitor_.DelayTask(FLAGS_timeout_bound * 1000,
                       boost::bind(&JobTracker::KeepMonitoring, this));
}

}
}

