#include "gru.h"

#include <vector>
#include <queue>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <cmath>

#include "galaxy_handler.h"
#include "resource_manager.h"
#include "common/dag_scheduler.h"
#include "common/file.h"
#include "common/rpc_client.h"
#include "proto/minion.pb.h"
#include "proto/serialize.pb.h"
#include "thread_pool.h"
#include "logging.h"

DECLARE_int32(replica_begin);
DECLARE_int32(replica_begin_percent);
DECLARE_int32(replica_num);
DECLARE_int32(left_percent);
DECLARE_int32(first_sleeptime);
DECLARE_int32(time_tolerance);
DECLARE_string(temporary_dir);

namespace baidu {
namespace shuttle {

// Interface Gru, implement common functions
class BasicGru : public Gru {
public:
    // General initialization
    BasicGru(JobDescriptor& job, const std::string& job_id, int node) :
            manager_(NULL), job_id_(job_id),  rpc_client_(NULL), job_(job), state_(kPending),
            galaxy_(NULL), node_(node), cur_node_(NULL), total_tasks_(0),
            start_time_(0), finish_time_(0), killed_(0), failed_(0), end_game_begin_(0),
            allow_duplicates_(true), need_dismissed_(0), dismissed_(0), next_phase_begin_(0),
            nearly_finish_callback_(0), finished_callback_(0), monitor_(1) {
        rpc_client_ = new RpcClient();
        galaxy_ = new GalaxyHandler(job_, job_id_, node_);
        cur_node_ = job_.mutable_nodes(node_);
        allow_duplicates_ = cur_node_->allow_duplicates();
        monitor_.AddTask(boost::bind(&BasicGru::KeepMonitoring, this));
    }
    virtual ~BasicGru() {
        if (rpc_client_ != NULL) {
            delete rpc_client_;
            rpc_client_ = NULL;
        }
    }

    virtual Status Start();
    virtual Status Kill();
    virtual ResourceItem* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    virtual Status GetHistory(std::vector<AllocateItem>& buf);
    virtual JobState GetState() {
        MutexLock lock(&meta_mu_);
        return state_;
    }
    virtual time_t GetStartTime() {
        // start_time_ generated at beginning and is read-only ever since
        return start_time_;
    }
    virtual time_t GetFinishTime() {
        MutexLock lock(&meta_mu_);
        return finish_time_;
    }
    virtual TaskStatistics GetStatistics();
    virtual GruType GetType() {
        return type_;
    }

    virtual Status SetCapacity(int capacity);
    virtual Status SetPriority(const std::string& priority);

    // Notice: Be sure to use these callback registration before gru starts
    virtual void RegisterNearlyFinishCallback(const boost::function<void ()>& callback) {
        nearly_finish_callback_ = callback;
    }

    virtual void RegisterFinishedCallback(const boost::function<void (JobState)>& callback) {
        finished_callback_ = callback;
    }

    virtual Status Load(const std::string& serialized);
    virtual std::string Dump();

protected:
    // Inner Interface for every gru to implement
    virtual ResourceManager* BuildResourceManager() = 0;
    virtual void DumpResourceManager(GruCollection* backup) = 0;
    virtual void LoadResourceManager(const GruCollection& backup) = 0;

    // Non-interface methods, which resemble in every gru
    void KeepMonitoring();
    void BuildEndGameCounters();
    void CancelOtherAttempts(int no, int finished_attempt);
    void CancelCallback(const CancelTaskRequest* request,
            CancelTaskResponse* response, bool fail, int eno);
    void ShutDown(JobState state);

protected:
    // Initialized to NULL since every gru differs
    ResourceManager* manager_;
    // Carefully initialized
    std::string job_id_;
    // Carefully initialized
    RpcClient* rpc_client_;
    // Carefully initialized
    // XXX Gru only modifies its own data, so no need to lock outside
    // TODO Need consideration
    JobDescriptor& job_;
    // NOT initialized
    GruType type_;
    Mutex meta_mu_;
    JobState state_;
    // Carefully initialized
    GalaxyHandler* galaxy_;
    // Carefully initialized
    int node_;
    // Carefully initialized
    NodeConfig* cur_node_;
    // Initialized to 0 since it depends on the resource manager
    int total_tasks_;
    // Carefully initialized in Start()
    time_t start_time_;
    time_t finish_time_;
    int killed_;
    int failed_;
    // Decide when the end game strategy is coming into effect
    // Initialized to 0 since it depends on resource sum
    int end_game_begin_;
    // Carefully initialized
    bool allow_duplicates_;
    // Dismiss a few minions when there's not much left for minions
    // Carefully initialized
    int need_dismissed_;
    // Carefully initialized
    int dismissed_;
    // When this many tasks is done, the next phase is supposed to be pulled up
    // Carefully initialized
    int next_phase_begin_;
    boost::function<void ()> nearly_finish_callback_;
    boost::function<void (JobState)> finished_callback_;

    // Allocation process is not relevant to derived class
    Mutex alloc_mu_;
    bool monitoring_;
    ThreadPool monitor_;
    std::queue<int> slugs_;
    std::vector<int> failed_count_;
    std::vector< std::vector<AllocateItem*> > allocation_table_;
    std::priority_queue<AllocateItem*, std::vector<AllocateItem*>,
                        AllocateItemComparator> time_heap_;
};

// First level gru, handling input files and writing output data to temporary directory
class AlphaGru : public BasicGru {
public:
    AlphaGru(JobDescriptor& job, const std::string& job_id, int node, DagScheduler* scheduler);
    virtual ~AlphaGru() { }

    virtual bool CleanTempDir();

protected:
    void NormalizeDfsinfo(std::vector<DfsInfo>& infos);

    virtual ResourceManager* BuildResourceManager();
    virtual void DumpResourceManager(GruCollection* backup);
    virtual void LoadResourceManager(const GruCollection& backup);
};

// Middle level gru, whose inputs and outputs are all kept in temporary directory
class BetaGru : public BasicGru {
public:
    BetaGru(JobDescriptor& job, const std::string& job_id, int node, DagScheduler* scheduler);
    virtual ~BetaGru() { }

    virtual bool CleanTempDir();
protected:
    virtual ResourceManager* BuildResourceManager();
    virtual void DumpResourceManager(GruCollection*) { }
    virtual void LoadResourceManager(const GruCollection& backup);
};

// Last level gru, dealing with temporary stored inputs and directing outputs to final directory
class OmegaGru : public BasicGru {
public:
    OmegaGru(JobDescriptor& job, const std::string& job_id, int node, DagScheduler* scheduler);
    virtual ~OmegaGru() { }

    virtual bool CleanTempDir();
protected:
    virtual ResourceManager* BuildResourceManager();
    virtual void DumpResourceManager(GruCollection*) { }
    virtual void LoadResourceManager(const GruCollection& backup);
};

// ----- Implementations start now -----

Gru* Gru::GetAlphaGru(JobDescriptor& job, const std::string& job_id,
        int node, DagScheduler* scheduler) {
    return new AlphaGru(job, job_id, node, scheduler);
}

Gru* Gru::GetBetaGru(JobDescriptor& job, const std::string& job_id,
        int node, DagScheduler* scheduler) {
    return new BetaGru(job, job_id, node, scheduler);
}

Gru* Gru::GetOmegaGru(JobDescriptor& job, const std::string& job_id,
        int node, DagScheduler* scheduler) {
    return new OmegaGru(job, job_id, node, scheduler);
}

Status BasicGru::Start() {
    start_time_ = std::time(NULL);

    // Check the existence of output
    File::Param param = File::BuildParam(cur_node_->output());
    File* fs = File::Create(kInfHdfs, param);
    if (fs->Exist(cur_node_->output().path())) {
        LOG(WARNING, "node %d output exists, failed: %s", node_, job_id_.c_str());
        cur_node_->set_total(0);
        return kWriteFileFail;
    }

    manager_ = BuildResourceManager();
    if (manager_ == NULL) {
        state_ = kFailed;
        return kNoMore;
    }

    // Limit capacity
    int limit = manager_->SumOfItems();
    limit += limit / 2;
    if (cur_node_->capacity() > limit) {
        cur_node_->set_capacity(limit);
    }

    BuildEndGameCounters();
    if (galaxy_->Start() != kOk) {
        state_ = kFailed;
        LOG(WARNING, "galaxy report error when submitting a new job: %s", job_id_.c_str());
        return kGalaxyError;
    }
    LOG(INFO, "start a new phase, node %d: %s", node_, job_id_.c_str());
    return kOk;
}

Status BasicGru::Kill() {
    JobState shutdown_state = kKilled;
    meta_mu_.Lock();
    if (state_ == kPending || state_ == kRunning) {
        state_ = kKilled;
    }
    shutdown_state = state_;
    meta_mu_.Unlock();
    // Call registered callback function for killed gru
    if (shutdown_state == kKilled && finished_callback_ != 0) {
        finished_callback_(kKilled);
    }
    ShutDown(shutdown_state);
    return kOk;
}

ResourceItem* BasicGru::Assign(const std::string& endpoint, Status* status) {
    // This is a lock-free state access since in any case this statement
    //    would not go wrong
    if (state_ == kPending) {
        state_ = kRunning;
    }
    ResourceItem* cur = manager_->GetItem();
    alloc_mu_.Lock();

    // For end game duplication
    if (allow_duplicates_ && cur != NULL && cur->no >= end_game_begin_) {
        for (int i = 0; i < FLAGS_replica_num; ++i) {
            slugs_.push(cur->no);
        }
    }

    // Check slugs queue to duplicate long-tail tasks
    while (cur == NULL) {
        while (!slugs_.empty() && manager_->IsAllocated(slugs_.front())) {
            LOG(DEBUG, "node %d slug pop %d: %s", node_, slugs_.front(), job_id_.c_str());
            slugs_.pop();
        }
        if (slugs_.empty()) {
            alloc_mu_.Unlock();
            meta_mu_.Lock();
            if (dismissed_ > 0 && dismissed_ >= need_dismissed_) {
                LOG(DEBUG, "node %d assign: suspend: %s", node_, job_id_.c_str());
                if (status != NULL) {
                    *status = kSuspend;
                }
            } else {
                ++dismissed_;
                LOG(DEBUG, "node %d assign: no more: %s", node_, job_id_.c_str());
                if (status != NULL) {
                    *status = kNoMore;
                }
            }
            meta_mu_.Unlock();
            return NULL;
        }
        LOG(INFO, "node %d duplicates %d task: %s", node_, slugs_.front(), job_id_.c_str());
        cur = manager_->GetCertainItem(slugs_.front());
        slugs_.pop();
    }

    // Pull up monitor
    if (cur->no >= end_game_begin_ && !monitoring_) {
        monitor_.AddTask(boost::bind(&BasicGru::KeepMonitoring, this));
        monitoring_ = true;
    }
    alloc_mu_.Unlock();

    // Prepare allocation record block and insert
    AllocateItem* alloc = new AllocateItem();
    alloc->no = cur->no;
    alloc->attempt = cur->attempt;
    alloc->endpoint = endpoint;
    alloc->state = kTaskRunning;
    alloc->alloc_time = std::time(NULL);
    alloc->period = -1;

    MutexLock lock(&alloc_mu_);
    allocation_table_[cur->no].push_back(alloc);
    time_heap_.push(alloc);
    LOG(INFO, "node %d assign map: < no - %d, attempt - %d >, to %s: %s",
            node_, cur->no, cur->attempt, endpoint.c_str(), job_id_.c_str());
    if (status != NULL) {
        *status = kOk;
    }
    return cur;
}

Status BasicGru::Finish(int no, int attempt, TaskState state) {
    AllocateItem* cur = NULL;
    try {
        MutexLock lock(&alloc_mu_);
        cur = allocation_table_.at(no).at(attempt);
    } catch (const std::out_of_range&) {
        LOG(WARNING, "node %d try to finish an inexist task: < no - %d, attempt - %d >: %s",
                node_, no, attempt, job_id_.c_str());
        return kNoMore;
    }

    LOG(INFO, "node %d finish a task: < no - %d, attempt - %d >, state %s: %s",
            node_, cur->no, cur->attempt, TaskState_Name(state).c_str(), job_id_.c_str());
    if (state == kTaskMoveOutputFailed) {
        if (!manager_->IsDone(cur->no)) {
            state = kTaskFailed;
        } else {
            state = kTaskCanceled;
        }
    }

    switch (state) {
    case kTaskCompleted:
        if (!manager_->FinishItem(cur->no)) {
            // Maybe this item has been finished or killed
            LOG(WARNING, "node %d ignores finish request < no - %d, attempt - %d >: %s",
                    node_, cur->no, cur->attempt, job_id_.c_str());
            state = kTaskCanceled;
            break;
        }
        int completed = manager_->Done();
        // Free some minions to release some resources when phase is coming to an end
        meta_mu_.Lock();
        need_dismissed_ = cur_node_->capacity() - static_cast<int>(
                ::ceil((cur_node_->total() - completed) * FLAGS_left_percent / 100.0));
        meta_mu_.Unlock();

        LOG(INFO, "node %d complete No.%d task(%d/%d): %s", node_, cur->no,
                completed, total_tasks_, job_id_.c_str());
        if (completed == next_phase_begin_) {
            // Pull up next phase and let next phase's minion prepare environment in advance
            LOG(INFO, "node %d nearly ends, pull up next phase in advance: %s",
                    node_, job_id_.c_str());
            if (nearly_finish_callback_ != 0) {
                nearly_finish_callback_();
            }
        } else if (completed == total_tasks_) {
            // Finish the whole phase
            LOG(INFO, "node %d is finished, kill minions: %s", node_, job_id_.c_str());
            meta_mu_.Lock();
            state_ = kCompleted;
            meta_mu_.Unlock();
            if (finished_callback_ != 0) {
                finished_callback_(kCompleted);
            }
            galaxy_->Kill();
            ShutDown(kCompleted);
            // XXX Maybe free resource manager
        }
        break;
    case kTaskFailed: {
        manager_->ReturnBackItem(cur->no);
        // XXX Data format and protocol here is not compatible with master branch
        // XXX Needs attention
        int current_failed = 0;
        {
            MutexLock lock(&meta_mu_);
            ++failed_;
        }
        {
            MutexLock lock(&alloc_mu_);
            current_failed = ++failed_count_[cur->no];
        }
        if (current_failed >= cur_node_->retry()) {
            LOG(INFO, "node %d failed, kill job: %s", node_, job_id_.c_str());
            meta_mu_.Lock();
            state_ = kFailed;
            meta_mu_.Unlock();
            if (finished_callback_ != 0) {
                finished_callback_(kFailed);
            }
        }
        }
        break;
    case kTaskKilled:
        manager_->ReturnBackItem(cur->no);
        ++killed_;
        break;
    case kTaskCanceled: break;
    default:
        LOG(WARNING, "node %d got unfamiliar task finish status %d: %s",
                node_, state, job_id_.c_str());
        return kNoMore;
    }

    {
        MutexLock lock(&alloc_mu_);
        cur->state = state;
        cur->period = std::time(NULL) - cur->alloc_time;
    }
    if (state != kTaskCompleted || !allow_duplicates_) {
        return kOk;
    }

    CancelOtherAttempts(cur->no, cur->attempt);
    return kOk;
}

Status BasicGru::GetHistory(std::vector<AllocateItem>& buf) {
    buf.clear();
    MutexLock lock(&alloc_mu_);
    for (std::vector< std::vector<AllocateItem*> >::iterator it = allocation_table_.begin();
            it != allocation_table_.end(); ++it) {
        for (std::vector<AllocateItem*>::iterator jt = it->begin();
                jt != it->end(); ++it) {
            buf.push_back(*(*jt));
        }
    }
    return kOk;
}

TaskStatistics BasicGru::GetStatistics() {
    int pending = 0, running = 0, completed = 0;
    if (manager_ != NULL) {
        pending = manager_->Pending();
        running = manager_->Allocated();
        completed = manager_->Done();
    }
    MutexLock lock(&meta_mu_);
    TaskStatistics task;
    task.set_total(total_tasks_);
    task.set_pending(pending);
    task.set_running(running);
    task.set_failed(failed_);
    task.set_killed(killed_);
    task.set_completed(completed);
    return task;
}

Status BasicGru::SetCapacity(int capacity) {
    if (galaxy_ == NULL) {
        MutexLock lock(&meta_mu_);
        if (state_ == kRunning || state_ == kPending) {
            cur_node_->set_capacity(capacity);
            return kOk;
        }
        return kGalaxyError;
    }
    if (galaxy_->SetCapacity(capacity) != kOk) {
        return kGalaxyError;
    }
    MutexLock lock(&meta_mu_);
    cur_node_->set_capacity(capacity);
    return kOk;
}

Status BasicGru::SetPriority(const std::string& priority) {
    if (galaxy_ == NULL) {
        return kGalaxyError;
    }
    if (galaxy_->SetPriority(priority) != kOk) {
        return kGalaxyError;
    }
    return kOk;
}

Status BasicGru::Load(const std::string& serialized) {
    GruCollection backup_gru;
    if (!backup_gru.ParseFromString(serialized)) {
        return kInvalidArg;
    }
    state_ = backup_gru.state();
    start_time_ = backup_gru.start_time();
    finish_time_ = backup_gru.finish_time();
    if (backup_gru.has_galaxy_handler()) {
        galaxy_->Load(backup_gru.galaxy_handler());
    } else {
        delete galaxy_;
        galaxy_ = NULL;
    }
    LoadResourceManager(backup_gru);
    if (manager_ == NULL) {
        state_ = kFailed;
        return kNoMore;
    }
    ::google::protobuf::RepeatedPtrField<GruCollection_Duplications>::const_iterator it;
    for (it = backup_gru.nos().begin(); it != backup_gru.nos().end(); ++it) {
        allocation_table_.push_back(std::vector<AllocateItem*>());
        std::vector<AllocateItem*>& cur_no = allocation_table_.back();
        ::google::protobuf::RepeatedPtrField<BackupAllocateItem>::const_iterator jt;
        for (jt = it->attempts().begin(); jt != it->attempts().end(); ++jt) {
            AllocateItem* cur = new AllocateItem();
            cur->no = jt->no();
            cur->attempt = jt->attempt();
            cur->endpoint = jt->endpoint();
            cur->state = jt->state();
            cur->alloc_time = jt->alloc_time();
            cur->period = jt->period();
            cur_no.push_back(cur);
        }
    }
    BuildEndGameCounters();
    return kOk;
}

std::string BasicGru::Dump() {
    GruCollection backup_gru;
    MutexLock lock1(&meta_mu_);
    MutexLock lock2(&alloc_mu_);
    backup_gru.set_state(state_);
    backup_gru.set_start_time(start_time_);
    backup_gru.set_finish_time(finish_time_);
    if (galaxy_ != NULL) {
        backup_gru.set_galaxy_handler(galaxy_->Dump());
    }
    DumpResourceManager(&backup_gru);
    for (std::vector< std::vector<AllocateItem*> >::iterator it = allocation_table_.begin();
            it != allocation_table_.end(); ++it) {
        GruCollection_Duplications* cur_no = backup_gru.add_nos();
        for (std::vector<AllocateItem*>::iterator jt = it->begin();
                jt != it->end(); ++jt) {
            BackupAllocateItem* cur = cur_no->add_attempts();
            AllocateItem* item = *jt;
            cur->set_no(item->no);
            cur->set_attempt(item->attempt);
            cur->set_endpoint(item->endpoint);
            cur->set_state(item->state);
            cur->set_alloc_time(item->alloc_time);
            cur->set_period(item->period);
        }
    }
    std::string serialized;
    backup_gru.SerializeToString(&serialized);
    return serialized;
}

void BasicGru::KeepMonitoring() {
    // Dynamic determination of delay check
    LOG(INFO, "[monitor] node %d monitor starts to check timeout: %s",
            node_, job_id_.c_str());
    std::vector<int> time_used;
    alloc_mu_.Lock();
    std::vector< std::vector<AllocateItem*> >::iterator it;
    for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
        for (std::vector<AllocateItem*>::iterator jt = it->begin();
                jt != it->end(); ++jt) {
            if ((*jt)->state == kTaskCompleted) {
                time_used.push_back((*jt)->period);
            }
        }
    }
    alloc_mu_.Unlock();
    time_t timeout = 0;
    if (!time_used.empty()) {
        std::sort(time_used.begin(), time_used.end());
        timeout = time_used[time_used.size() / 2];
        // 20% time tolerance
        timeout += timeout / 5; 
        LOG(INFO, "[monitor] node %d calc timeout bound, timeout = %ld: %s",
                node_, timeout, job_id_.c_str());
    } else {
        monitor_.DelayTask(FLAGS_first_sleeptime * 1000,
                boost::bind(&BasicGru::KeepMonitoring, this));
        LOG(INFO, "[monitor] node %d will now rest for %ds: %s",
                node_, FLAGS_first_sleeptime, job_id_.c_str());
    }
    bool is_long_task = timeout >= FLAGS_time_tolerance;

    // timeout will NOT be 0 since monitor will be terminated if no tasks is finished
    time_t sleep_time = std::min((time_t)FLAGS_time_tolerance, timeout);
    unsigned int counter = is_long_task ? -1 : 10;

    std::vector<AllocateItem*> returned_item;
    alloc_mu_.Lock();
    time_t now = std::time(NULL);
    while (counter-- != 0 && !time_heap_.empty()) {
        AllocateItem* top = time_heap_.top();
        // To soon to check status or duplicate
        if (now - top->alloc_time < sleep_time) {
            break;
        }
        time_heap_.pop();

        if (top->state != kTaskRunning) {
            ++counter;
            continue;
        }
        // Check status in non-duplication mode or when it's not ready to duplicate
        if (!allow_duplicates_ || (now - top->alloc_time < timeout)) {
            QueryRequest request;
            QueryResponse response;
            Minion_Stub* stub = NULL;
            rpc_client_->GetStub(top->endpoint, &stub);
            boost::scoped_ptr<Minion_Stub> stub_guard(stub);
            LOG(INFO, "[monitor] node %d will query %s with < no - %d, attempt - %d >: %s",
                    node_, top->endpoint.c_str(), top->no, top->attempt, job_id_.c_str());
            alloc_mu_.Unlock();
            bool ok = rpc_client_->SendRequest(stub, &Minion_Stub::Query,
                    &request, &response, 5, 1);
            alloc_mu_.Lock();
            if (ok && response.job_id() == job_id_ && response.task_id() == top->no &&
                    response.attempt_id() == top->attempt) {
                // Seems fine, return item to time heap
                ++counter;
                returned_item.push_back(top);
                continue;
            }
            if (ok && !manager_->IsAllocated(top->no)) {
                if (top->state == kTaskRunning) {
                    top->state = kTaskKilled;
                    top->period = std::time(NULL) - top->alloc_time;
                    ++killed_;
                }
                ++counter;
                continue;
            }

            LOG(INFO, "[monitor] node %d queried an error, returned %s, "
                    "< no - %d, attempt - %d>: %s", node_, ok ? "ok" : "error",
                    response.task_id(), response.attempt_id(), job_id_.c_str());
            top->state = kTaskKilled;
            top->period = std::time(NULL) - top->alloc_time;
            ++killed_;
            manager_->ReturnBackItem(top->no);
        }

        slugs_.push(top->no);
        LOG(INFO, "[monitor] node %d reallocated a long no-response task: "
                "< no - %d, attempt - %d >: %s", node_, top->no, top->attempt, job_id_.c_str());
        LOG(DEBUG, "[monitor] node %d report, sizeof(slugs_) = %u: %s",
                node_, slugs_.size(), job_id_.c_str());
    }
    // Return the well-working items
    for (std::vector<AllocateItem*>::iterator it = returned_item.begin();
            it != returned_item.end(); ++it) {
        time_heap_.push(*it);
    }
    alloc_mu_.Unlock();

    monitor_.DelayTask(sleep_time * 1000, boost::bind(&BasicGru::KeepMonitoring, this));
    LOG(INFO, "[monitor] node %d will now rest for %ds: %s",
            node_, FLAGS_first_sleeptime, job_id_.c_str());
}

void BasicGru::BuildEndGameCounters() {
    if (manager_ == NULL) {
        return;
    }
    total_tasks_ = manager_->SumOfItems();
    end_game_begin_ = total_tasks_ - FLAGS_replica_begin;
    int temp = total_tasks_ - total_tasks_ * FLAGS_replica_begin_percent / 100;
    if (end_game_begin_ > temp) {
        end_game_begin_ = temp;
    }
    next_phase_begin_ = end_game_begin_;
}

void BasicGru::CancelCallback(const CancelTaskRequest* request,
        CancelTaskResponse* response, bool fail, int eno) {
    if (fail) {
        LOG(WARNING, "node %d failed to cancel < no - %d, attempt - %d >, err %d: %s",
                node_, request->task_id(), request->attempt_id(), eno, job_id_.c_str());
    }
    delete request;
    delete response;
}

void BasicGru::CancelOtherAttempts(int no, int finished_attempt) {
    if (static_cast<size_t>(no) > allocation_table_.size()) {
        return;
    }
    Minion_Stub* stub = NULL;
    MutexLock lock(&alloc_mu_);
    const std::vector<AllocateItem*>& cur_attempts = allocation_table_[no];
    for (std::vector<AllocateItem*>::const_iterator it = cur_attempts.begin();
            it != cur_attempts.end(); ++it) {
        AllocateItem* candidate = *it;
        if (candidate->attempt == finished_attempt) {
            continue;
        }
        candidate->state = kTaskCanceled;
        candidate->period = std::time(NULL) - candidate->alloc_time;
        rpc_client_->GetStub(candidate->endpoint, &stub);
        boost::scoped_ptr<Minion_Stub> stub_guard(stub);
        LOG(INFO, "node %d cancels < no - %d, attempt - %d > task: %s",
                node_, candidate->no, candidate->attempt, job_id_.c_str());
        CancelTaskRequest* request = new CancelTaskRequest();
        CancelTaskResponse* response = new CancelTaskResponse();
        request->set_job_id(job_id_);
        request->set_task_id(candidate->no);
        request->set_attempt_id(candidate->attempt);
        boost::function<void (const CancelTaskRequest*, CancelTaskResponse*, bool, int) > callback;
        callback = boost::bind(&BasicGru::CancelCallback, this, _1, _2, _3, _4);
        rpc_client_->AsyncRequest(stub, &Minion_Stub::CancelTask,
                request, response, callback, 2, 1);
    }
}

void BasicGru::ShutDown(JobState state) {
    meta_mu_.Lock();
    if (galaxy_ != NULL) {
        LOG(INFO, "node %d phase finished, kill: %s", node_, job_id_.c_str());
        delete galaxy_;
        galaxy_ = NULL;
    }
    state_ = state;
    monitor_.Stop(true);
    meta_mu_.Unlock();

    if (state != kCompleted) {
        alloc_mu_.Lock();
        for (std::vector< std::vector<AllocateItem*> >::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            for (std::vector<AllocateItem*>::iterator jt = it->begin();
                    jt != it->end(); ++jt) {
                AllocateItem* cur = *jt;
                if (cur->state == kTaskRunning) {
                    cur->state = kTaskKilled;
                    cur->period = std::time(NULL) - cur->alloc_time;
                    ++killed_;
                }
            }
        }
    }
    alloc_mu_.Unlock();
    finish_time_ = std::time(NULL);
}

AlphaGru::AlphaGru(JobDescriptor& job, const std::string& job_id,
        int node, DagScheduler* scheduler) : BasicGru(job, job_id, node) {
    type_ = kAlphaGru;
    // For map-only job, use alpha gru without generate temp dir
    if (!cur_node_->has_output()) {
        return;
    }
    const std::vector<int>& dest = scheduler->Destinations();
    // XXX Currently use the first output dfs info for all temporary directory
    //     May need a better way
    if (!job.nodes(dest[0]).has_output()) {
        return;
    }
    const DfsInfo& output = job.nodes(dest[0]).output();
    File::Param param = File::BuildParam(output);
    File* fs = File::Create(kInfHdfs, param);
    std::string temp;
    File::ParseFullAddress(output.path(),
            /* type */NULL, /* host */NULL, /* port */NULL, &temp);
    temp += FLAGS_temporary_dir + "node_output_" + boost::lexical_cast<std::string>(node);
    fs->Mkdir(temp);
    delete fs;

    cur_node_->mutable_output()->CopyFrom(output);
    cur_node_->mutable_output()->set_path(temp);
    const std::vector<int>& nexts = scheduler->NextNodes(node);
    for (std::vector<int>::const_iterator it = nexts.begin();
            it != nexts.end(); ++it) {
        DfsInfo* cur = job.mutable_nodes(*it)->add_inputs();
        cur->CopyFrom(output);
        cur->set_path(temp);
    }
}

bool AlphaGru::CleanTempDir() {
    if (!cur_node_->has_output()) {
        return true;
    }
    // For map-only job, do not clean the output dir
    if (!cur_node_->output().path().find(FLAGS_temporary_dir) == std::string::npos) {
        return true;
    }
    File::Param param = File::BuildParam(cur_node_->output());
    File* fs = File::Create(kInfHdfs, param);
    bool ok = fs->Remove(cur_node_->output().path());
    delete fs;

    cur_node_->clear_output();
    return ok;
}

void AlphaGru::NormalizeDfsinfo(std::vector<DfsInfo>& infos) {
    std::vector<DfsInfo>::iterator it = infos.begin();
    while (it != infos.end()) {
        // TODO use more universal method
        const std::string& path = it->path();
        if (boost::starts_with(path, "hdfs://")) {
            if (path.find_first_of(':', 7) == std::string::npos) {
                it = infos.erase(it);
            } else {
                std::string host, port;
                File::ParseFullAddress(path, NULL, &host, &port, NULL);
                it->set_host(host);
                it->set_port(port);
                ++it;
            }
        } else {
            if (it->has_host() && it->has_port()) {
                std::string path = "hdfs://" + it->host() + ":" +
                    boost::lexical_cast<std::string>(it->port()) + it->path();
                it->set_path(path);
                ++it;
            } else {
                it = infos.erase(it);
            }
        }
    }
}

ResourceManager* AlphaGru::BuildResourceManager() {
    std::vector<DfsInfo> dfs_inputs;
    const ::google::protobuf::RepeatedPtrField<DfsInfo>& inputs = cur_node_->inputs();
    dfs_inputs.resize(inputs.size());
    std::copy(inputs.begin(), inputs.end(), dfs_inputs.begin());

    NormalizeDfsinfo(dfs_inputs);
    if (dfs_inputs.size() != static_cast<size_t>(inputs.size())) {
        return NULL;
    }

    ResourceManager* manager = NULL;
    if (cur_node_->input_format() == kNLineInput) {
        manager = ResourceManager::GetNLineManager(dfs_inputs);
    } else {
        manager = ResourceManager::GetBlockManager(dfs_inputs, job_.split_size());
    }
    if (manager == NULL || manager->SumOfItems() < 1) {
        LOG(INFO, "node %d phase cannot divide input, which may not exist: %s",
                node_, job_id_.c_str());
        cur_node_->set_total(0);
        state_ = kFailed;
        if (!finished_callback_) {
            finished_callback_(kFailed);
        }
        return NULL;
    }
    cur_node_->set_total(manager_->SumOfItems());
    return manager;
}

void AlphaGru::DumpResourceManager(GruCollection* backup) {
    const std::vector<ResourceItem>& resources = manager_->Dump();
    for (std::vector<ResourceItem>::const_iterator it = resources.begin();
            it != resources.end(); ++it) {
        BackupResourceItem* cur = backup->add_resources();
        cur->set_no(it->no);
        cur->set_attempt(it->attempt);
        cur->set_status(it->status);
        cur->set_input_file(it->input_file);
        cur->set_offset(it->offset);
        cur->set_size(it->size);
        cur->set_allocated(it->allocated);
    }
}

void AlphaGru::LoadResourceManager(const GruCollection& backup) {
    std::vector<ResourceItem> resources;
    ::google::protobuf::RepeatedPtrField<BackupResourceItem>::const_iterator it;
    for (it = backup.resources().begin(); it != backup.resources().end(); ++it) {
        ResourceItem item;
        item.type = kFileItem;
        item.no = it->no();
        item.attempt = it->attempt();
        item.status = static_cast<ResourceStatus>(it->status());
        item.input_file = it->input_file();
        item.offset = it->offset();
        item.size = it->size();
        item.allocated = it->allocated();
        resources.push_back(item);
    }
    manager_ = ResourceManager::BuildManagerFromBackup(resources);
}

BetaGru::BetaGru(JobDescriptor& job, const std::string& job_id,
        int node, DagScheduler* scheduler) : BasicGru(job, job_id, node) {
    type_ = kBetaGru;
    const std::vector<int>& dest = scheduler->Destinations();
    // XXX Currently use the first output dfs info for all temporary directory
    //     May need a better way
    if (!job.nodes(dest[0]).has_output()) {
        return;
    }
    const DfsInfo& output = job.nodes(dest[0]).output();
    File::Param param = File::BuildParam(output);
    File* fs = File::Create(kInfHdfs, param);
    std::string temp;
    File::ParseFullAddress(output.path(),
            /* type */NULL, /* host */NULL, /* port */NULL, &temp);
    temp += FLAGS_temporary_dir + "node_output_" + boost::lexical_cast<std::string>(node);
    fs->Mkdir(temp);
    delete fs;

    cur_node_->mutable_output()->CopyFrom(output);
    cur_node_->mutable_output()->set_path(temp);
    const std::vector<int>& nexts = scheduler->NextNodes(node);
    for (std::vector<int>::const_iterator it = nexts.begin();
            it != nexts.end(); ++it) {
        DfsInfo* cur = job.mutable_nodes(*it)->add_inputs();
        cur->CopyFrom(output);
        cur->set_path(temp);
    }
}

bool BetaGru::CleanTempDir() {
    cur_node_->clear_inputs();
    if (!cur_node_->has_output()) {
        return true;
    }
    File::Param param = File::BuildParam(cur_node_->output());
    File* fs = File::Create(kInfHdfs, param);
    bool ok = fs->Remove(cur_node_->output().path());
    delete fs;

    cur_node_->clear_output();
    return ok;
}

ResourceManager* BetaGru::BuildResourceManager() {
    return ResourceManager::GetIdManager(cur_node_->total());
}

void BetaGru::LoadResourceManager(const GruCollection& /*backup*/) {
    manager_ = ResourceManager::GetIdManager(cur_node_->total());
}

OmegaGru::OmegaGru(JobDescriptor& job, const std::string& job_id,
        int node, DagScheduler* /*scheduler*/) : BasicGru(job, job_id, node) {
    type_ = kOmegaGru;
}

bool OmegaGru::CleanTempDir() {
    cur_node_->clear_inputs();
    return true;
}

ResourceManager* OmegaGru::BuildResourceManager() {
    return ResourceManager::GetIdManager(cur_node_->total());
}

void OmegaGru::LoadResourceManager(const GruCollection& /*backup*/) {
    manager_ = ResourceManager::GetIdManager(cur_node_->total());
}

}
}

