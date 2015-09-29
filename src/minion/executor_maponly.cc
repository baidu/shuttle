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

    FileSystem* fs = FileSystem::CreateInfHdfs();
    boost::scoped_ptr<FileSystem> fs_guard(fs);
    FileSystem::Param param;
    const std::string temp_file_name = GetMapWorkFilename(task);
    bool ok = fs->Open(temp_file_name, param, kWriteFile);
    if (!ok) {
        LOG(WARNING, "create output file fail, %s", temp_file_name.c_str());
        return kTaskFailed;
    }

    const size_t buf_size = 40960;
    char* buf = (char*)malloc(buf_size);
    while (!feof(user_app)) {
        size_t n_read = fread(buf, sizeof(char), buf_size, user_app);
        if (n_read != buf_size ) {
            if (feof(user_app)) {
                ok = fs->WriteAll(buf, n_read);
                break;
            } else if (ferror(user_app) !=0 ) {
                LOG(WARNING, "errors occur in reading, %s", strerror(errno));
                ok = false;
                break;
            } else{
                assert(0);
            }
        }
        ok = fs->WriteAll(buf, n_read);
        if (!ok) {
            break;
        }
    }
    free(buf);
    if (!ok || !fs->Close()) {
        LOG(WARNING, "write data fail, %s", temp_file_name.c_str());
        return kTaskFailed;
    }
    int ret = pclose(user_app);
    if (ret != 0) {
        LOG(WARNING, "user app fail, cmd is %s, ret: %d", cmd.c_str(), ret);
        return kTaskFailed;
    }
    if (!MoveTempToOutput(task, fs, true)) {
        return kTaskFailed;
    }
    return kTaskCompleted;
}

}
}
