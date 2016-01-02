#include "gru.h"

#include <vector>
#include <queue>
#include <map>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include "galaxy_handler.h"
#include "resource_manager.h"
#include "common/rpc_client.h"
#include "common/tools_util.h"
#include "thread_pool.h"
#include "logging.h"

DECLARE_int32(replica_begin);
DECLARE_int32(replica_begin_percent);
DECLARE_int32(replica_num);

namespace baidu {
namespace shuttle {

// Interface Gru
template <class Resource>
class BasicGru : Gru<Resource> {
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
        monitor_.AddTask(boost::bind(&BasicGru<Resource>::KeepMonitoring, this));
    }
    virtual ~BasicGru() {
        if (rpc_client_ != NULL) {
            delete rpc_client_;
            rpc_client_ = NULL;
        }
    }

    virtual Status Start();
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

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
    virtual BasicResourceManager<Resource>* BuildResourceManager() = 0;

    // Non-interface methods, which resemble in every gru
    void KeepMonitoring();
    void SerializeAllocationTable(std::vector<AllocateItem>& buf);
    void BuildEndGameCounters();

protected:
    // Initialized to NULL since every gru differs
    BasicResourceManager<Resource>* manager_;
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
    // Initialized to 0 since it depends on resource sum
    int end_game_begin_;
    // Carefully initialized
    bool allow_duplicates_;
    boost::function<void ()> nearly_finish_callback_;
    boost::function<void ()> finished_callback_;

    Mutex alloc_mu_;
    ThreadPool monitor_;
    std::queue<int> slugs_;
    std::vector<int> failed_count_;
    std::vector< std::vector<AllocateItem*> > allocation_table_;
    std::priority_queue<AllocateItem*, std::vector<AllocateItem*>,
                        AllocateItemComparator> time_heap_;
};

class AlphaGru : public BasicGru<ResourceItem> {
public:
    AlphaGru(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~AlphaGru();
protected:
    virtual BasicResourceManager<ResourceItem>* BuildResourceManager();
};

class BetaGru : public BasicGru<IdItem> {
public:
    BetaGru(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~BetaGru();
protected:
    virtual BasicResourceManager<IdItem>* BuildResourceManager();
};

class OmegaGru : public BasicGru<IdItem> {
public:
    OmegaGru(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~OmegaGru();
protected:
    virtual BasicResourceManager<IdItem>* BuildResourceManager();
};

// ----- Implementations start now -----

template <class Resource>
Gru<ResourceItem>* Gru<Resource>::GetAlphaGru(JobDescriptor& job,
        const std::string& job_id, int node) {
    return new AlphaGru(job, job_id, node);
}

template <class Resource>
Gru<IdItem>* Gru<Resource>::GetBetaGru(JobDescriptor& job,
        const std::string& job_id, int node) {
    return new BetaGru(job, job_id, node);
}

template <class Resource>
Gru<IdItem>* Gru<Resource>::GetOmegaGru(JobDescriptor& job,
        const std::string& job_id, int node) {
    return new OmegaGru(job, job_id, node);
}

template <class Resource>
Status BasicGru<Resource>::Start() {
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

template <class Resource>
Status BasicGru<Resource>::Kill() {
    meta_mu_.Lock();
    if (galaxy_ != NULL) {
        LOG(INFO, "node %d phase finished, kill: %s", node_, job_id_.c_str());
        delete galaxy_;
        galaxy_ = NULL;
    }
    monitor_.Stop(true);
    if (state_ != kCompleted) {
        state_ = kKilled;
    }
    meta_mu_.Unlock();

    // TODO Cancel every task
    finish_time_ = std::time(NULL);
    return kOk;
}

template <class Resource>
Resource* BasicGru<Resource>::Assign(const std::string& endpoint, Status* status) {
    // This is a lock-free state access since in any case this statement
    //    would not go wrong
    if (state_ == kPending) {
        state_ = kRunning;
    }
    Resource* cur = manager_->GetItem();
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
            // TODO Suspend vs NoMore
            return NULL;
        }
        LOG(INFO, "node %d duplicates %d task: %s", node_, slugs_.front(), job_id_.c_str());
        cur = manager_->GetCertainItem(slugs_.front());
        slugs_.pop();
    }
    alloc_mu_.Unlock();
    // TODO Pull up monitor

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

template <class Resource>
Status BasicGru<Resource>::Finish(int no, int attempt, TaskState state) {
    AllocateItem* cur = NULL;
    try {
        cur = allocation_table_[no][attempt];
    } catch (const std::out_of_range&) {
        LOG(WARNING, "node %d try to finish an inexist task: < no - %d, attempt - %d >: %s",
                node_, no, attempt, job_id_.c_str());
        return kNoMore;
    }
    if (state == kTaskMoveOutputFailed) {
        if (!manager_->IsDone(cur->no)) {
            state = kTaskFailed;
        } else {
            state = kTaskCanceled;
        }
    }

    switch (state) {
    case kTaskCompleted:
        // TODO More implement here
        break;
    case kTaskFailed:
        manager_->ReturnBackItem(cur->no);
        ++failed_count_[cur->no]; ++failed_;
        if (failed_count_[cur->no] >= cur_node_->retry()) {
            LOG(INFO, "node %d failed, kill job: %s", node_, job_id_.c_str());
            // TODO Retract job
            // need mu_
            state_ = kFailed;
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
    // TODO Cancel tasks
    return kOk;
}

template <class Resource>
TaskStatistics BasicGru<Resource>::GetStatistics() {
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

template <class Resource>
Status BasicGru<Resource>::SetCapacity(int capacity) {
    if (galaxy_ == NULL) {
        return kGalaxyError;
    }
    if (galaxy_->SetCapacity(capacity) != kOk) {
        return kGalaxyError;
    }
    cur_node_->set_capacity(capacity);
    return kOk;
}

template <class Resource>
Status BasicGru<Resource>::SetPriority(const std::string& priority) {
    if (galaxy_ == NULL) {
        return kGalaxyError;
    }
    if (galaxy_->SetPriority(priority) != kOk) {
        return kGalaxyError;
    }
    return kOk;
}

template <class Resource>
void BasicGru<Resource>::KeepMonitoring() {
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
    // TODO More implementation here
}

template <class Resource>
void BasicGru<Resource>::SerializeAllocationTable(std::vector<AllocateItem>& buf) {
    std::vector< std::vector<AllocateItem*> >::iterator it;
    MutexLock lock(&alloc_mu_);
    for (it = allocation_table_.begin(); it != allocation_table_.end(); ++it) {
        for (std::vector<AllocateItem*>::iterator jt = it->begin();
                jt != it->end(); ++jt) {
            buf.push_back(*(*jt));
        }
    }
}

template <class Resource>
void BasicGru<Resource>::BuildEndGameCounters() {
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

AlphaGru::AlphaGru(JobDescriptor& job, const std::string& job_id, int node) :
        BasicGru<ResourceItem>(job, job_id, node) {
    // TODO Initialize AlphaGru
    //   Generate temp output dir
}

BasicResourceManager<ResourceItem>* AlphaGru::BuildResourceManager() {
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
        manager = new NLineResourceManager(inputs,input_param);
    } else {
        manager = new ResourceManager(inputs, input_param, job_.split_size());
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
        BasicGru<IdItem>(job, job_id, node) {
    // TODO Initialize BetaGru
    //   Generate temp output dir
}

BasicResourceManager<IdItem>* BetaGru::BuildResourceManager() {
    return new IdManager(cur_node_->total());
}

OmegaGru::OmegaGru(JobDescriptor& job, const std::string& job_id, int node) :
        BasicGru<IdItem>(job, job_id, node) {
    // TODO Initialize BetaGru
}

BasicResourceManager<IdItem>* OmegaGru::BuildResourceManager() {
    return new IdManager(cur_node_->total());
}

} // namespace shuttle
} // namespace baidu

