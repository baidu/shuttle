#ifndef _BAIDU_SHUTTLE_GRU_H_
#define _BAIDU_SHUTTLE_GRU_H_
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

// Interface Gru
template <class Resource>
class Gru {
public:
    // Operations
    virtual Status Start() = 0;
    virtual Status Update(const std::string& priority, int capacity) = 0;
    virtual Status Kill() = 0;
    virtual Resource* Assign(const std::string& endpoint, Status* status) = 0;
    virtual Status Finish(int no, int attempt, TaskState state) = 0;

    // Data getters
    virtual time_t GetStartTime() = 0;
    virtual time_t GetFinishTime() = 0;
    virtual TaskStatistics GetStatistics() = 0;
    virtual Status GetHistory(const std::vector& buf) = 0;

    // For backup and recovery
    // Load()
    // Dump()
};

class AlphaGru : Gru {
};

class BetaGru : Gru {
};

class OmegaGru : Gru {
};

}
}

#endif

