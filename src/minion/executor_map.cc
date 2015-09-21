#include "executor.h"

namespace baidu {
namespace shuttle {

MapExecutor::MapExecutor() {

}

MapExecutor::~MapExecutor() {

}

TaskState MapExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map task");
    return kTaskCompleted;
}

}
}
