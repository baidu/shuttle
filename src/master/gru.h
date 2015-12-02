#ifndef _BAIDU_SHUTTLE_GRU_H_
#define _BAIDU_SHUTTLE_GRU_H_
#include <string>
#include <vector>
#include <queue>
#include "galaxy_handler.h"
#include "proto/shuttle.pb.h"
#include "mutex.h"
#include "thread_pool.h"

namespace baidu {
namespace shuttle {

// For AlphaGru to manage inputs
class ResourceItem;
// For BetaGru and OmegaGru to manage task-id
class IdItem;

struct AllocateItem {
    int no;
    int attempt;
    std::string endpoint;
    TaskState state;
    time_t alloc_time;
    time_t period;
};

// For time heap to justify the timestamp
struct AllocateItemComparator {
    bool operator()(AllocateItem* const& litem, AllocateItem* const& ritem) const {
        return litem->alloc_time > ritem->alloc_time;
    }
};

class RpcClient;

// Interface Gru
template <class Resource>
class Gru {
public:
    // General initialization
    Gru(const JobDescriptor& job, int node) :
            manager_(NULL), rpc_client_(NULL), job_(job), galaxy_(NULL),
            node_(node), total_tasks_(0), start_time_(0), end_time_(0),
            killed_(0), failed_(0), end_game_begin_(0), monitor_(1) {
        rpc_client_ = new RpcClient();
        start_time_ = std::time(NULL);
        end_time_ = start_time_ - 1;
    }
    virtual ~Gru() {
        if (rpc_client_ != NULL) {
            delete rpc_client_;
            rpc_client_ = NULL;
        }
    }
    // Operations
    virtual Status Start() = 0;
    virtual Status Update(const std::string& priority, int capacity) = 0;
    virtual Status Kill() = 0;
    virtual Resource* Assign(const std::string& endpoint, Status* status) = 0;
    virtual Status Finish(int no, int attempt, TaskState state) = 0;

    // Data getters
    virtual Status GetHistory(const std::vector& buf) const = 0;

    // Non-interface part
    virtual time_t GetStartTime() const {
        // start_time_ generated at beginning and is read-only ever since
        return start_time_;
    }
    virtual time_t GetFinishTime() const {
        MutexLock lock(&meta_mu_);
        return end_time_;
    }
    virtual TaskStatistics GetStatistics() const {
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

    // For backup and recovery
    // Load()
    // Dump()

protected:
    // TODO template don't support outside definition.
    // so maybe consider a new method, since this monitor
    // can be all the same in all grus.
    void KeepMonitoring();

protected:
    BasicResourceManager<Resource>* manager_;
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
    // Carefully initialized
    time_t end_time_;
    int killed_;
    int failed_;
    int end_game_begin_;

    Mutex alloc_mu_;
    ThreadPool monitor_;
    std::queue<int> slugs_;
    std::vector<int> failed_count;
    std::vector<AllocateItem*> allocation_table_;
    std::priority_queue<AllocateItem*, std::vector<AllocateItem*>,
                        AllocateItemComparator> time_heap_;
};

class AlphaGru : public Gru<ResourceItem> {
public:
    // Operations
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    // Data getters
    virtual Status GetHistory(const std::vector& buf) const;
};

class BetaGru : public Gru<IdItem> {
public:
    // Operations
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    // Data getters
    virtual Status GetHistory(const std::vector& buf) const;
};

class OmegaGru : public Gru<IdItem> {
public:
    // Operations
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    // Data getters
    virtual Status GetHistory(const std::vector& buf) const;
};

}
}

#endif

