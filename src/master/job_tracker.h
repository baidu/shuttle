#ifndef _BAIDU_SHUTTLE_JOB_TRACKER_H_
#define _BAIDU_SHUTTLE_JOB_TRACKER_H_
#include <string>

#include "galaxy.h"
#include "mutex.h"
#include "proto/shuttle.pb.h"
#include "proto/app_master.pb.h"
#include "resource_manager.h"

namespace baidu {
namespace shuttle {

class JobTracker {

public:
    JobTracker(::baidu::galaxy::Galaxy* galaxy_sdk, const JobDescriptor& job);
    virtual ~JobTracker();

    Status Start();
    Status Update(const std::string& priority, int map_capacity, int reduce_capacity);
    Status Kill();
    ResourceItem* Assign(const std::string& endpoint);
    Status FinishTask(int no, int attempt, TaskState state);

    const std::string& GetJobId() {
        return job_id_;
    }
    const JobDescriptor& GetJobDescriptor() {
        return job_descriptor_;
    }
    JobState GetState() {
        return state_;
    }

private:
    ::baidu::galaxy::Galaxy* sdk_;
    Mutex mu_;
    JobDescriptor job_descriptor_;
    std::string job_id_;
    JobState state_;
    ResourceManager* resource_;
    std::string map_minion_;
    ::baidu::galaxy::JobDescription map_description_;
    std::string reduce_minion_;
    ::baidu::galaxy::JobDescription reduce_description_;
};

}
}

#endif
