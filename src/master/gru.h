#ifndef _BAIDU_SHUTTLE_GRU_H_
#define _BAIDU_SHUTTLE_GRU_H_
#include <string>
#include <boost/function.hpp>
#include "proto/shuttle.pb.h"
#include "proto/app_master.pb.h"
#include "mutex.h"

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

template <class Resource>
class Gru {
public:
    // Operations
    virtual Status Start() = 0;
    virtual Status Update(const std::string& priority, int capacity) = 0;
    virtual Status Kill() = 0;
    virtual Resource* Assign(const std::string& endpoint, Status* status) = 0;
    virtual Status Finish(int no, int attempt, TaskState state) = 0;

    // Data getters
    virtual Status GetHistory(const std::vector<AllocateItem>& buf) const = 0;
    virtual time_t GetStartTime() const = 0;
    virtual time_t GetFinishTime() const = 0;
    virtual TaskStatistics GetStatistics() const = 0;

    // Notify upper job tracker the nearly finish and finish state,
    // so that the job tracker could pull up next phrase or change some states
    virtual void RegisterNearlyFinishCallback(boost::function<void ()> callback) = 0;
    virtual void RegisterFinishedCallback(boost::function<void ()> callback) = 0;

    // For backup and recovery
    // Load()
    // Dump()
    
    // Factory methods
    // TODO Give appropriate parameters for initialization
    static Gru<ResourceItem>* GetAlphaGru();
    static Gru<IdItem>* GetBetaGru();
    static Gru<IdItem>* GetOmegaGru();
};


}
}

#endif

