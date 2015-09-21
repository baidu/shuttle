#include "executor.h"

namespace baidu {
namespace shuttle {

Executor::Executor() {

}

Executor::~Executor() {

}

Executor* Executor::GetExecutor(WorkMode mode) {
    Executor* executor;
    switch(mode) {
    case kMap:
        executor = new MapExecutor();
        break;
    case kReduce:
        executor = new ReduceExecutor();
        break;
    case kMapOnly:
        executor = new MapOnlyExecutor();
        break;
    default:
        LOG(FATAL, "unkonw work mode: %d", mode);
        abort();
        break;
    }
    return executor;
}

}
}
