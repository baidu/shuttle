#include "executor.h"
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <boost/algorithm/string.hpp>
#include "partition.h"

namespace baidu {
namespace shuttle {

const static int32_t sLineBufSize = 40960;

MapExecutor::MapExecutor() : line_buf_(NULL) {
    ::setenv("mapred_task_is_map", "true", 1);
    line_buf_ = (char*)malloc(sLineBufSize);
}

MapExecutor::~MapExecutor() {
    free(line_buf_);
}

TaskState MapExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map task");
    std::string cmd = "sh ./app_wrapper.sh " + task.job().map_command();
    FILE* user_app = popen(cmd.c_str(), "r");
    if (user_app == NULL) {
        LOG(WARNING, "start user app fail, cmd is %s, (%s)", 
            cmd.c_str(), strerror(errno));
        return kTaskFailed;
    }

    KeyFieldBasedPartitioner key_field_partition(task);
    IntHashPartitioner int_hash_partition(task);
    Partitioner* partitioner = &key_field_partition;
    if (task.job().partition() == kIntHashPartitioner) {
        partitioner =  &int_hash_partition;
    }

    while (!feof(user_app)) {
        if (fgets(line_buf_, sLineBufSize, user_app) == NULL) {
            break;
        }
        std::string line(line_buf_);
        std::string key;
        int reduce_no;
        if (line.size() > 0 && line[line.size()-1] == '\n') {
            line.erase(line.size()-1);
        }
        reduce_no = partitioner->Calc(line, &key);
    }
    return kTaskCompleted;
}

}
}
