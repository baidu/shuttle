#ifndef _BAIDU_SHUTTLE_GRU_H_
#define _BAIDU_SHUTTLE_GRU_H_
#include <string>
#include <boost/function.hpp>
#include "proto/shuttle.pb.h"
#include "proto/app_master.pb.h"
#include "mutex.h"

namespace baidu {
namespace shuttle {

// Item returned from resource manager
class ResourceItem;

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

class Gru {
public:
    // Operations
    virtual Status Start() = 0;
    virtual Status Kill() = 0;
    virtual ResourceItem* Assign(const std::string& endpoint, Status* status) = 0;
    virtual Status Finish(int no, int attempt, TaskState state) = 0;

    // Data getters
    virtual Status GetHistory(std::vector<AllocateItem>& buf) = 0;
    virtual JobState GetState() = 0;
    virtual time_t GetStartTime() = 0;
    virtual time_t GetFinishTime() = 0;
    virtual TaskStatistics GetStatistics() = 0;
    
    // Property setters
    virtual Status SetCapacity(int capacity) = 0;
    virtual Status SetPriority(const std::string& priority) = 0;

    // Notify upper job tracker the nearly finish and finish state,
    // so that the job tracker could pull up next phrase or change some states
    virtual void RegisterNearlyFinishCallback(const boost::function<void ()>& callback) = 0;
    virtual void RegisterFinishedCallback(const boost::function<void (JobState)>& callback) = 0;

    // For backup and recovery
    // Load()
    // Dump()
    
    // Factory methods
    // TODO Give appropriate parameters for initialization
    static Gru* GetAlphaGru(JobDescriptor& job, const std::string& job_id, int node);
    static Gru* GetBetaGru(JobDescriptor& job, const std::string& job_id, int node);
    static Gru* GetOmegaGru(JobDescriptor& job, const std::string& job_id, int node);
};


}
}

#endif

