#ifndef _BAIDU_SHUTTLE_JOB_TRACKER_H_
#define _BAIDU_SHUTTLE_JOB_TRACKER_H_
#include <string>
#include <vector>
#include <ctime>
#include "gru.h"
#include "dag_scheduler.h"
#include "proto/shuttle.pb.h"
#include "proto/master.pb.h"

namespace baidu {
namespace shuttle {

struct UpdateItem {
    int node;
    int capacity;
};

// Manage different phase of a job
class JobTracker {

public:
    JobTracker(const JobDescriptor& job_descriptor);
    ~JobTracker();

    Status Start();
    Status Update(const std::string& priority, const std::vector<UpdateItem>& nodes);
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
    Status GetStatistics(std::vector<TaskStatistics>& stats);
    // Notice: Currently input info is not included
    Status GetTaskOverview(std::vector<TaskOverview>& tasks);

    // Serialize and unserialize meta info
    Status Load(const std::string& serialized);
    std::string Dump();

    // Register callback for MasterImpl to clean up garbage and deal with the status
    void RegisterFinishedCallback(boost::function<void ()> callback) {
        finished_callback_ = callback;
    }

private:
    std::string GenerateJobId();
    // For gru callback
    void ScheduleNextPhase(int node);
    void FinishPhase(int node, JobState state);
    void FinishWholeJob(JobState state);

private:
    JobDescriptor job_;
    std::string job_id_;

    Mutex meta_mu_;
    JobState state_;
    time_t start_time_;
    time_t finish_time_;
    boost::function<void ()> finished_callback_;

    DagScheduler scheduler_;
    std::vector<Gru*> grus_;
};

}
}

#endif

