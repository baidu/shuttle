#include "gru.h"

#include <vector>
#include <queue>
#include <map>
#include <boost/bind.hpp>
#include "galaxy_handler.h"
#include "resource_manager.h"
#include "thread_pool.h"

namespace baidu {
namespace shuttle {

// Interface Gru
template <class Resource>
class BasicGru : Gru<Resource> {
public:
    // General initialization
    Gru(const JobDescriptor& job, const std::string& job_id, int node) :
            manager_(NULL), job_id_(job_id),  rpc_client_(NULL), job_(job),
            galaxy_(NULL), node_(node), total_tasks_(0), start_time_(0), end_time_(0),
            killed_(0), failed_(0), end_game_begin_(0), monitor_(1), allow_duplicates_(true) {
        rpc_client_ = new RpcClient();
        start_time_ = std::time(NULL);
        end_time_ = start_time_ - 1;
        monitor_.AddTask(boost::bind(&Gru<Resource>::KeepMonitoring, this));
    }
    virtual ~Gru() {
        if (rpc_client_ != NULL) {
            delete rpc_client_;
            rpc_client_ = NULL;
        }
    }

    virtual time_t GetStartTime() const {
        // start_time_ generated at beginning and is read-only ever since
        return start_time_;
    }
    virtual time_t GetFinishTime() const {
        MutexLock lock(&meta_mu_);
        return end_time_;
    }
    virtual TaskStatistics GetStatistics() const;

protected:
    void KeepMonitoring();
    void SerializeAllocationTable(std::vector& buf);

protected:
    // Initialized to NULL since every gru differs
    BasicResourceManager<Resource>* manager_;
    // Carefully initialized
    std::string job_id_;
    // Carefully initialized
    RpcClient* rpc_client_;
    // Carefully initialized
    const JobDescriptor& job_;
    Mutex meta_mu_;
    // Initialized to NULL since every gru differs in galaxy
    GalaxyHandler* galaxy_;
    // Carefully initialized
    int node_;
    // Initialized to 0 since it depends on the gru
    int total_tasks_;
    // Carefully initialized
    time_t start_time_;
    time_t end_time_;
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
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);
};

class BetaGru : public BasicGru<IdItem> {
public:
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);
};

class OmegaGru : public BasicGru<IdItem> {
public:
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);
};

// ----- Implementations start now -----

template <class Resource>
TaskStatistics BasicGru<Resource>::GetStatistics() {
    int pending = 0, running = 0, completed = 0;
    if (manager_ != NULL) {
        pending = manager->Pending();
        running = manager->Allocated();
        completed = manager->Done();
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
        for (std::vector<AllocateItem>::iterator jt = it->begin();
                jt != it->end(); ++jt) {
            if ((*jt)->state == kTaskCompleted) {
                time_used.push_back((*jt)->period);
            }
        }
    }
    alloc_mu_.Unlock();
    // TODO More implementation here
}

} // namespace shuttle
} // namespace baidu

