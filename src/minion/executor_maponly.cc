#include "executor.h"
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <boost/scoped_ptr.hpp>
#include "sort/filesystem.h"

namespace baidu {
namespace shuttle {

MapOnlyExecutor::MapOnlyExecutor() {
    ::setenv("mapred_task_is_map", "true", 1);
    ::setenv("mapred_task_is_maponly", "true", 1);
}

MapOnlyExecutor::~MapOnlyExecutor() {

}

TaskState MapOnlyExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map-only task");
    std::string cmd = "sh ./app_wrapper.sh " + task.job().map_command();
    FILE* user_app = popen(cmd.c_str(), "r");
    if (user_app == NULL) {
        LOG(WARNING, "start user app fail, cmd is %s, (%s)",
            cmd.c_str(), strerror(errno));
        return kTaskFailed;
    }

    FileSystem::Param param;
    FillParam(param, task);
    const std::string temp_file_name = GetMapWorkFilename(task);

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
    if (!MoveTempToOutput(task, fs, true)) {
        LOG(WARNING, "fail to move output");
        return kTaskMoveOutputFailed;
    }
    return kTaskCompleted;
}

}
}

