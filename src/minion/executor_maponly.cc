#include "executor.h"

namespace baidu {
namespace shuttle {

MapOnlyExecutor::MapOnlyExecutor() {

}

MapOnlyExecutor::~MapOnlyExecutor() {

}

TaskState MapOnlyExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map-only task");
    return kTaskCompleted;
}

}
}
