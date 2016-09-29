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
    Executor(const std::string& job_id, const JobDescriptor& job, const TaskInfo& info);
    ~Executor();

    TaskState Exec();
    Status Stop(int32_t task_id);
    bool ParseCounters(const std::string& work_dir,
            std::map<std::string, int64_t>& counters);
private:
    void SetEnv();
    bool PrepareOutputDir();
    bool MoveTempDir();
private:
    const std::string& job_id_;
    const TaskInfo& task_;
    const JobDescriptor& job_;
    const NodeConfig& node_;
    std::string final_dir_;
    std::string work_dir_;
    pid_t wrapper_pid_;
    DagScheduler scheduler_;
};

}
}

#endif

