#include "executor.h"
#include <unistd.h>
#include <errno.h>
#include <boost/scoped_ptr.hpp>
#include "common/filesystem.h"

namespace baidu {
namespace shuttle {

ReduceExecutor::ReduceExecutor() {
    ::setenv("mapred_task_is_map", "false", 1);
}

ReduceExecutor::~ReduceExecutor() {

}

TaskState ReduceExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec reduce task");
    ::setenv("mapred_work_output_dir", GetReduceWorkDir(task).c_str(), 1);
    std::string cmd = "sh ./app_wrapper.sh \"" + task.job().reduce_command() + "\"";
    LOG(INFO, "reduce command is: %s", cmd.c_str());
    FILE* user_app = popen(cmd.c_str(), "r");
    if (user_app == NULL) {
        LOG(WARNING, "start user app fail, cmd is %s, (%s)", 
            cmd.c_str(), strerror(errno));
        return kTaskFailed;
    }

    FileSystem::Param param;
    FillParam(param, task);
    const std::string temp_file_name = GetReduceWorkFilename(task);

    if (task.job().output_format() == kTextOutput) {
        TaskState status = TransTextOutput(user_app, temp_file_name, param, task);
        if (status != kTaskCompleted) {
            return status;
        }
    } else if (task.job().output_format() == kBinaryOutput) {
        TaskState status = TransBinaryOutput(user_app, temp_file_name, param, task);
        if (status != kTaskCompleted) {
            return status;
        }
    } else if (task.job().output_format() == kSuffixMultipleTextOutput) {
        TaskState status = TransMultipleTextOutput(user_app, temp_file_name, param, task);
        if (status != kTaskCompleted) {
            return status;
        }
    } else {
        LOG(FATAL, "unknown output format");
    }
    int ret = pclose(user_app);
    if (ret != 0) {
        LOG(WARNING, "user app fail, cmd is %s, ret: %d", cmd.c_str(), ret);
        return kTaskFailed;
    }
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    boost::scoped_ptr<FileSystem> fs_guard(fs);
    if (task.job().output_format() == kSuffixMultipleTextOutput) {
        if (!MoveMultipleTempToOutput(task, fs, false)) {
            LOG(WARNING, "fail to move multiple output");
            return kTaskMoveOutputFailed;
        } 
    } else {
        if (!MoveTempToOutput(task, fs, false)) {
            LOG(WARNING, "fail to move output");
            return kTaskMoveOutputFailed;
        }
    }
    return kTaskCompleted;
}

}
}
