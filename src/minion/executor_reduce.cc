#include "executor.h"

namespace baidu {
namespace shuttle {

ReduceExecutor::ReduceExecutor() {

}

ReduceExecutor::~ReduceExecutor() {

}

TaskState ReduceExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec reduce task");
    return kTaskCompleted;
}

}
}
