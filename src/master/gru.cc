#include "gru.h"

#include <vector>
#include <queue>
#include <map>
#include <boost/bind.hpp>
#include "galaxy_handler.h"
#include "resource_manager.h"
#include "thread_pool.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

// Interface Gru
template <class Resource>
class BasicGru : Gru<Resource> {
public:
    // General initialization
    BasicGru(JobDescriptor& job, const std::string& job_id, int node) :
            manager_(NULL), job_id_(job_id),  rpc_client_(NULL), job_(job), state_(kPending),
            galaxy_(NULL), node_(node), total_tasks_(0), start_time_(0), finish_time_(0),
            killed_(0), failed_(0), end_game_begin_(0), monitor_(1), allow_duplicates_(true) {
        rpc_client_ = new RpcClient();
        monitor_.AddTask(boost::bind(&Gru<Resource>::KeepMonitoring, this));
    }
    virtual ~BasicGru() {
        if (rpc_client_ != NULL) {
            delete rpc_client_;
            rpc_client_ = NULL;
        }
    }

    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();

    virtual time_t GetStartTime() const {
        // start_time_ generated at beginning and is read-only ever since
        return start_time_;
    }
    virtual time_t GetFinishTime() const {
        MutexLock lock(&meta_mu_);
        return finish_time_;
    }
    virtual TaskStatistics GetStatistics() const;

protected:
    // Inner Interface for every gru to implement
    virtual BasicResourceManager<Resource>* BuildResourceManager() = 0;
    virtual GalaxyHandler* BuildGalaxyHandler() = 0;

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
    // Initialized to 0 since it depends on the gru
    int total_tasks_;
    // Carefully initialized in Start()
    time_t start_time_;
    time_t finish_time_;
    int killed_;
    int failed_;
    // Initialized to 0 since it depends on resource sum
    int end_game_begin_;
    // Initialized to true since it depends on job descriptor
    bool allow_duplicates_;

    Mutex alloc_mu_;
    ThreadPool monitor_;
    std::queue<int> slugs_;
    std::vector<int> failed_count;
    std::vector< std::vector<AllocateItem*> > allocation_table_;
    std::priority_queue<AllocateItem*, std::vector<AllocateItem*>,
                        AllocateItemComparator> time_heap_;
};

class AlphaGru : public BasicGru<ResourceItem> {
public:
    AlphaGru();
    virtual ~AlphaGru();
    virtual ResourceItem* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);
protected:
    virtual BasicResourceManager<ResourceItem>* BuildResourceManager();
    virtual GalaxyHandler* BuildGalaxyHandler();
};

class BetaGru : public BasicGru<IdItem> {
public:
    BetaGru();
    virtual ~BetaGru();
    virtual IdItem* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);
protected:
    virtual BasicResourceManager<IdItem>* BuildResourceManager();
    virtual GalaxyHandler* BuildGalaxyHandler();
};

class OmegaGru : public BasicGru<IdItem> {
public:
    OmegaGru();
    virtual ~OmegaGru();
    virtual IdItem* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);
protected:
    virtual BasicResourceManager<IdItem>* BuildResourceManager();
    virtual GalaxyHandler* BuildGalaxyHandler();
};

// ----- Implementations start now -----

template <class Resource>
Status BasicGru<Resource>::Start() {
    start_time_ = std::time(NULL);
    if (BuildResourceManager() != kOk) {
        return kNoMore;
    }
    BuildEndGameCounters();
    galaxy_ = BuildGalaxyHandler();
    if (galaxy_->Start() == kOk) {
        LOG(INFO, "start a new phase, node %d: %s", node_, job_id_.c_str());
        return kOk;
    }
    LOG(WARNING, "galaxy report error when submitting a new job: %s", job_id_.c_str());
    return kGalaxyError;
}

template <class Resource>
Status BasicGru<Resource>::Update(const std::string& priority, int capacity) {
    if (galaxy_ == NULL) {
        return;
    }
    if (galaxy_->Update(priority, capacity) != kOk) {
        return kGalaxyError;
    }
    if (capacity != -1) {
        // TODO Set capacity
    }
    if (!priority.empty()) {
        // TODO Set priority
    }
}

template <class Resource>
Status BasicGru<Resource>::Kill() {
    meta_mu_.Lock();
    if (galaxy_ != NULL) {
        LOG(INFO, "node %d phase finished, kill: %s", job_id_.c_str());
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
TaskStatistics BasicGru<Resource>::GetStatistics() const {
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
    // TODO Build end game counters here
}

} // namespace shuttle
} // namespace baidu

