#ifndef _BAIDU_SHUTTLE_CLUSTER_HANDLER_H_
#define _BAIDU_SHUTTLE_CLUSTER_HANDLER_H_
#include <string>
#include <stdint.h>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class ClusterHandler {
public:
    ClusterHandler()
        : additional_millicores(default_millicores),
          additional_memory(default_memory) { }
    virtual ~ClusterHandler() { }

    virtual Status Start() = 0;
    virtual Status Kill() = 0;

    virtual Status SetPriority(const std::string& priority) = 0;
    virtual Status SetCapacity(int capacity) = 0;

    virtual Status Load(const std::string& cluster_jobid) = 0;
    virtual std::string Dump() = 0;

    int32_t additional_millicores;
    int64_t additional_memory;

private:
    static const int32_t default_millicores = 0;
    static const int64_t default_memory = 1024L * 1024 * 1024;
};

}
}

#endif

