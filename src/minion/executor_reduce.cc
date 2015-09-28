#include "executor.h"
#include <unistd.h>

namespace baidu {
namespace shuttle {

ReduceExecutor::ReduceExecutor() {
	::setenv("mapred_task_is_map", "false", 1);
}

ReduceExecutor::~ReduceExecutor() {

}

TaskState ReduceExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec reduce task");
    return kTaskCompleted;
}

}
}
