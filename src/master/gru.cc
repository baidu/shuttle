#include "gru.h"

#include <vector>
#include <queue>
#include <map>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <cmath>

#include "galaxy_handler.h"
#include "resource_manager.h"
#include "common/rpc_client.h"
#include "common/tools_util.h"
#include "proto/minion.pb.h"
#include "thread_pool.h"
#include "logging.h"

DECLARE_int32(replica_begin);
DECLARE_int32(replica_begin_percent);
DECLARE_int32(replica_num);
DECLARE_int32(first_sleeptime);
DECLARE_int32(time_tolerance);

namespace baidu {
namespace shuttle {

// Interface Gru
class BasicGru : public Gru {
public:
    // General initialization
    BasicGru(JobDescriptor& job, const std::string& job_id, int node) :
            manager_(NULL), job_id_(job_id),  rpc_client_(NULL), job_(job), state_(kPending),
            galaxy_(NULL), node_(node), cur_node_(NULL), total_tasks_(0),
            start_time_(0), finish_time_(0), killed_(0), failed_(0), end_game_begin_(0),
            allow_duplicates_(true), nearly_finish_callback_(0), finished_callback_(0), monitor_(1) {
        rpc_client_ = new RpcClient();
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

    virtual Status SetCapacity(int capacity);
    virtual Status SetPriority(const std::string& priority);

    virtual void RegisterNearlyFinishCallback(boost::function<void ()> callback) {
        MutexLock lock(&meta_mu_);
        nearly_finish_callback_ = callback;
    }

    virtual void RegisterFinishedCallback(boost::function<void ()> callback) {
        MutexLock lock(&meta_mu_);
        finished_callback_ = callback;
    }

protected:
    // Inner Interface for every gru to implement
    virtual ResourceManager* BuildResourceManager() = 0;

    // Non-interface methods, which resemble in every gru
    void KeepMonitoring();
    void SerializeAllocationTable(std::vector<AllocateItem>& buf);
    void BuildEndGameCounters();
    void CancelOtherAttempts(int no, int finished_attempt);
    void CancelCallback(const CancelTaskRequest* request,
            CancelTaskResponse* response, bool fail, int eno);

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
    Mutex meta_mu_;
    JobState state_;
    // Initialized to NULL since every gru differs in galaxy
    GalaxyHandler* galaxy_;
    // Carefully initialized
    int node_;
    // Carefully initialized
    NodeConfig* cur_node_;
    // Initialized to 0 since it depends on the gru
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
    boost::function<void ()> finished_callback_;

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
    AlphaGru(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~AlphaGru();
protected:
    virtual ResourceManager* BuildResourceManager();
};

// Middle level gru, whose inputs and outputs are all kept in temporary directory
class BetaGru : public BasicGru {
public:
    BetaGru(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~BetaGru();
protected:
    virtual ResourceManager* BuildResourceManager();
};

// Last level gru, dealing with temporary stored inputs and directing outputs to final directory
class OmegaGru : public BasicGru {
public:
    OmegaGru(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~OmegaGru();
protected:
    virtual ResourceManager* BuildResourceManager();
};

// ----- Implementations start now -----

Gru* Gru::GetAlphaGru(JobDescriptor& job, const std::string& job_id, int node) {
    return new AlphaGru(job, job_id, node);
}

Gru* Gru::GetBetaGru(JobDescriptor& job, const std::string& job_id, int node) {
    return new BetaGru(job, job_id, node);
}

Gru* Gru::GetOmegaGru(JobDescriptor& job, const std::string& job_id, int node) {
    return new OmegaGru(job, job_id, node);
}

Status BasicGru::Start() {
    start_time_ = std::time(NULL);
    manager_ = BuildResourceManager();
    if (manager_ == NULL) {
        return kNoMore;
    }
    BuildEndGameCounters();
    galaxy_ = new GalaxyHandler(job_, job_id_, node_);
    if (galaxy_->Start() == kOk) {
        LOG(INFO, "start a new phase, node %d: %s", node_, job_id_.c_str());
        return kOk;
    }
    LOG(WARNING, "galaxy report error when submitting a new job: %s", job_id_.c_str());
    return kGalaxyError;
}

Status BasicGru::Kill() {
    meta_mu_.Lock();
    if (galaxy_ != NULL) {
        LOG(INFO, "node %d phase finished, kill: %s", node_, job_id_.c_str());
        delete galaxy_;
        galaxy_ = NULL;
    }
    monitor_.Stop(true);
    if (state_ == kPending || state_ == kRunning) {
        state_ = kKilled;
    }
    meta_mu_.Unlock();

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
    alloc_mu_.Unlock();
    finish_time_ = std::time(NULL);
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
        cur = allocation_table_[no][attempt];
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
            LOG(WARNING, "node %d ignores finish request < no - %d, attempt - %d >: %s",
                    node_, cur->no, cur->attempt, job_id_.c_str());
            state = kTaskCanceled;
            break;
        }
        int completed = manager_->Done();
        // TODO Change need_dismissed

        LOG(INFO, "node %d complete No.%d task(%d/%d): %s", node_, cur->no,
                completed, manager_->SumOfItem(), job_id_.c_str());
        if (completed == next_phase_begin_) {
            LOG(INFO, "node %d nearly ends, pull up next phase in advance: %s",
                    node_, job_id_.c_str());
            // TODO Note this operation is lock-free, so callback function is vulnerable
            if (nearly_finish_callback_ != 0) {
                nearly_finish_callback_();
            }
        } else if (completed == manager_->SumOfItem()) {
            LOG(INFO, "node %d is finished, kill minions: %s", node_, job_id_.c_str());
            state_ = kCompleted;
            if (finished_callback_ != 0) {
                finished_callback_();
            }
            if (galaxy_ != NULL) {
                delete galaxy_;
                galaxy_ = NULL;
            }
            // TODO Maybe do more cleaning up
            // TODO Maybe free resource manager
        }
        break;
    case kTaskFailed:
        manager_->ReturnBackItem(cur->no);
        // XXX Data format and protocol here is not compatible with master branch
        // XXX Needs attention
        ++failed_count_[cur->no]; ++failed_;
        if (failed_count_[cur->no] >= cur_node_->retry()) {
            LOG(INFO, "node %d failed, kill job: %s", node_, job_id_.c_str());
            state_ = kFailed;
            if (finished_callback_ != 0) {
                finished_callback_();
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
        return kGalaxyError;
    }
    if (galaxy_->SetCapacity(capacity) != kOk) {
        return kGalaxyError;
    }
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

void BasicGru::SerializeAllocationTable(std::vector<AllocateItem>& buf) {
    std::vector< std::vector<AllocateItem*> >::iterator it;
    MutexLock lock(&alloc_mu_);
    for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
        for (std::vector<AllocateItem*>::iterator jt = it->begin();
                jt != it->end(); ++jt) {
            buf.push_back(*(*jt));
        }
    }
}

void BasicGru::BuildEndGameCounters() {
    if (manager_ == NULL) {
        return;
    }
    int items = manager_->SumOfItem();
    end_game_begin_ = items - FLAGS_replica_begin;
    int temp = items - items * FLAGS_replica_begin_percent / 100;
    if (end_game_begin_ > temp) {
        end_game_begin_ = temp;
    }
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

AlphaGru::AlphaGru(JobDescriptor& job, const std::string& job_id, int node) :
        BasicGru(job, job_id, node) {
    // TODO Initialize AlphaGru
    //   Generate temp output dir
}

ResourceManager* AlphaGru::BuildResourceManager() {
    std::vector<std::string> inputs;
    const ::google::protobuf::RepeatedPtrField<std::string>& input_filenames
        = cur_node_->inputs();
    inputs.reserve(input_filenames.size());
    std::copy(input_filenames.begin(), input_filenames.end(), inputs.begin());

    FileSystem::Param input_param;
    const DfsInfo& input_dfs = cur_node_->input_dfs();
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
        cur_node_->mutable_input_dfs()->set_host(host);
        cur_node_->mutable_input_dfs()->set_port(boost::lexical_cast<std::string>(port));
    } else if (!input_dfs.host().empty() && !input_dfs.port().empty()) {
        input_param["host"] = input_dfs.host();
        input_param["port"] = input_dfs.port();
    }

    ResourceManager* manager = NULL;
    if (cur_node_->input_format() == kNLineInput) {
        manager = ResourceManager::GetNLineManager(inputs,input_param);
    } else {
        manager = ResourceManager::GetBlockManager(inputs, input_param, job_.split_size());
    }
    if (manager == NULL || manager->SumOfItem() < 1) {
        LOG(INFO, "node %d phase cannot divide input, which may not exist: %s",
                node_, job_id_.c_str());
        cur_node_->set_total(0);
        state_ = kFailed;
        if (!finished_callback_) {
            finished_callback_();
        }
        return NULL;
    }
    cur_node_->set_total(manager_->SumOfItem());
    return manager;
}

BetaGru::BetaGru(JobDescriptor& job, const std::string& job_id, int node) :
        BasicGru(job, job_id, node) {
    // TODO Initialize BetaGru
    //   Generate temp output dir
}

ResourceManager* BetaGru::BuildResourceManager() {
    return ResourceManager::GetIdManager(cur_node_->total());
}

OmegaGru::OmegaGru(JobDescriptor& job, const std::string& job_id, int node) :
        BasicGru(job, job_id, node) {
    // TODO Initialize BetaGru
}

ResourceManager* OmegaGru::BuildResourceManager() {
    return ResourceManager::GetIdManager(cur_node_->total());
}

} // namespace shuttle
} // namespace baidu

