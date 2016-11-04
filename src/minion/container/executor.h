#ifndef _BAIDU_SHUTTLE_EXECUTOR_H_
#define _BAIDU_SHUTTLE_EXECUTOR_H_
#include <string>
#include <map>
#include <unistd.h>
#include "common/dag_scheduler.h"
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class Executor {
public:
    Executor(const std::string& job_id) : job_id_(job_id),
            job_(NULL), node_(NULL), task_(NULL),
            wrapper_pid_(-1), scheduler_(NULL) { }
    ~Executor() {
        Stop(task_->task_id());
        if (scheduler_ != NULL) {
            delete scheduler_;
            scheduler_ = NULL;
        }
    }

    TaskState Exec(const JobDescriptor& job, const TaskInfo& task);
    Status Stop(int32_t task_id);
    bool ParseCounters(std::map<std::string, int64_t>& counters);
private:
    void SetEnv();
    bool PrepareOutputDir();
    bool MoveTempDir();
private:
    const std::string& job_id_;
    JobDescriptor const* job_;
    NodeConfig const* node_;
    TaskInfo const* task_;
    std::string final_dir_;
    std::string work_dir_;
    pid_t wrapper_pid_;
    DagScheduler* scheduler_;
};

}
}

#endif

