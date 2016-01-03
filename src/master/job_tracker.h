#ifndef _BAIDU_SHUTTLE_JOB_TRACKER_H_
#define _BAIDU_SHUTTLE_JOB_TRACKER_H_
#include <string>
#include <vector>
#include <ctime>
#include "gru.h"
#include "dag_scheduler.h"
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class JobTracker {

public:
    JobTracker(const JobDescriptor& job_descriptor);
    virtual ~JobTracker();

    Status Start();
    // TODO Update interface under construction
    Status Update(...);
    Status Kill();
    
    ResourceItem* Assign(int node, const std::string& endpoint, Status* status);
    Status Finish(int node, int no, int attempt, TaskState state);

    std::string GetJobId() {
        return job_id_;
    }
    JobDescriptor GetJobDescriptor() {
        return job_;
    }
    JobState GetState() {
        return state_;
    }
    time_t GetStartTime() {
        return start_time_;
    }
    time_t GetFinishTime() {
        MutexLock lock(&meta_mu_);
        return finish_time_;
    }

private:
    std::string GenerateJobId();
    // For gru callback
    void ScheduleNextPhase(int node);
    void FinishPhase(int node);
    void FinishWholeJob();

private:
    JobDescriptor job_;
    std::string job_id_;

    Mutex meta_mu_;
    JobState state_;
    time_t start_time_;
    time_t finish_time_;

    DagScheduler scheduler_;
    std::vector<Gru*> grus_;
};

}
}

#endif

