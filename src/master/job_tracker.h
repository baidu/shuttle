#ifndef _BAIDU_SHUTTLE_JOB_TRACKER_H_
#define _BAIDU_SHUTTLE_JOB_TRACKER_H_
#include <string>
#include <list>
#include <queue>
#include <vector>
#include <utility>
#include <ctime>

#include "galaxy.h"
#include "mutex.h"
#include "thread_pool.h"
#include "proto/shuttle.pb.h"
#include "proto/app_master.pb.h"
#include "resource_manager.h"
#include "gru.h"
#include "common/rpc_client.h"
#include "common/filesystem.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

class MasterImpl;

struct AllocateItem {
    int resource_no;
    int attempt;
    std::string endpoint;
    TaskState state;
    time_t alloc_time;
    time_t period;
    bool is_map;
};

struct AllocateItemComparator {
    bool operator()(AllocateItem* const& litem, AllocateItem* const& ritem) const {
        return litem->alloc_time < ritem->alloc_time;
    }
};

class JobTracker {

public:
    JobTracker(MasterImpl* master, ::baidu::galaxy::Galaxy* galaxy_sdk,
               const JobDescriptor& job);
    virtual ~JobTracker();

    Status Start();
    Status Update(const std::string& priority, int map_capacity, int reduce_capacity);
    Status Kill();
    ResourceItem* AssignMap(const std::string& endpoint);
    IdItem* AssignReduce(const std::string& endpoint);
    Status FinishMap(int no, int attempt, TaskState state);
    Status FinishReduce(int no, int attempt, TaskState state);

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
    TaskStatistics GetMapStatistics();
    TaskStatistics GetReduceStatistics();

    Status Check(ShowJobResponse* response) {
        MutexLock lock(&alloc_mu_);
        for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
                it != allocation_table_.end(); ++it) {
            TaskOverview* task = response->add_tasks();
            TaskInfo* info = task->mutable_info();
            info->set_task_id((*it)->resource_no);
            info->set_attempt_id((*it)->attempt);
            info->set_task_type((job_descriptor_.job_type() == kMapOnlyJob) ? kMapOnly :
                    ((*it)->is_map ? kMap : kReduce));
            // XXX Warning: input will NOT return
            task->set_state((*it)->state);
            task->set_minion_addr((*it)->endpoint);
        }
        return kOk;
    }

private:
    void KeepMonitoring();
    std::string GenerateJobId();

private:
    MasterImpl* master_;
    ::baidu::galaxy::Galaxy* galaxy_;
    Mutex mu_;
    JobDescriptor job_descriptor_;
    std::string job_id_;
    JobState state_;
    // For non-duplication use
    bool predict_assign_;
    // Resource allocation
    Mutex alloc_mu_;
    std::list<AllocateItem*> allocation_table_;
    std::priority_queue<AllocateItem*, std::vector<AllocateItem*>,
                        AllocateItemComparator> time_heap_;
    std::vector<int> failed_count_;
    // Map resource
    Gru* map_;
    ResourceManager* map_manager_;
    int map_completed_;
    int map_end_game_begin_;
    std::queue<int> map_slug_;
    // Reduce resource
    Gru* reduce_;
    IdManager* reduce_manager_;
    int reduce_begin_;
    int reduce_completed_;
    int reduce_end_game_begin_;
    std::queue<int> reduce_slug_;
    // Thread for monitoring
    ThreadPool monitor_;
    int64_t monitor_id_;
    // To communicate with minion
    RpcClient* rpc_client_;
    // To check if output path is exists
    FileSystem* fs_;
};

}
}

#endif

