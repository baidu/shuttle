#include "executor.h"
#include <stdio.h>
#include <unistd.h>

namespace baidu {
namespace shuttle {

MapExecutor::MapExecutor() {
	::setenv("mapred_task_is_map", "true", 1);
}

MapExecutor::~MapExecutor() {

}

TaskState MapExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map task");
    return kTaskCompleted;
}

}
}
