#ifndef _BAIDU_SHUTTLE_JOB_TRACKER_H_
#define _BAIDU_SHUTTLE_JOB_TRACKER_H_
#include <string>
#include <list>
#include <queue>
#include <utility>
#include <ctime>

#include "galaxy.h"
#include "mutex.h"
#include "thread_pool.h"
#include "proto/shuttle.pb.h"
#include "proto/app_master.pb.h"
#include "resource_manager.h"
#include "rpc_client.h"

namespace baidu {
namespace shuttle {

struct AllocateItem {
    int resource_no;
    int attempt;
    std::string endpoint;
    TaskState state;
    time_t alloc_time;
};

struct AllocateItemComparator {
    bool operator()(AllocateItem* const& litem, AllocateItem* const& ritem) const {
        return litem->alloc_time < ritem->alloc_time;
    }
};

class JobTracker {

public:
    JobTracker(::baidu::galaxy::Galaxy* galaxy_sdk, const JobDescriptor& job);
    virtual ~JobTracker();

    Status Start();
    Status Update(const std::string& priority, int map_capacity, int reduce_capacity);
    Status Kill();
    ResourceItem* Assign(const std::string& endpoint);
    Status FinishTask(int no, int attempt, TaskState state);

    std::string GetJobId() {
        MutexLock lock(&mu_);
        return job_id_;
    }
    JobDescriptor GetJobDescriptor() {
        MutexLock lock(&mu_);
        return job_descriptor_;
    }
    JobState GetState() {
        MutexLock lock(&mu_);
        return state_;
    }
    TaskStatistics GetMapStatistics() {
        MutexLock lock(&alloc_mu_);
        return map_stat_;
    }
    TaskStatistics GetReduceStatistics() {
        MutexLock lock(&alloc_mu_);
        return reduce_stat_;
    }
private:
    void KeepMonitoring();

private:
    ::baidu::galaxy::Galaxy* sdk_;
    Mutex mu_;
    JobDescriptor job_descriptor_;
    std::string job_id_;
    JobState state_;
    // Resource allocation
    ResourceManager* resource_;
    Mutex alloc_mu_;
    std::list<AllocateItem*> allocation_table_;
    std::priority_queue<AllocateItem*, std::vector<AllocateItem*>,
                        AllocateItemComparator> time_heap_;
    // Map resource
    std::string map_minion_;
    ::baidu::galaxy::JobDescription map_description_;
    TaskStatistics map_stat_;
    // Reduce resource
    std::string reduce_minion_;
    ::baidu::galaxy::JobDescription reduce_description_;
    TaskStatistics reduce_stat_;
    // Thread for monitoring
    ThreadPool monitor_;
    // To communicate with minion
    RpcClient rpc_client_;
};

}
}

#endif

