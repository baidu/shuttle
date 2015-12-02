#ifndef _BAIDU_SHUTTLE_GRU_H_
#define _BAIDU_SHUTTLE_GRU_H_
#include <string>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

// For AlphaGru to manage inputs
class ResourceItem;
// For BetaGru and OmegaGru to manage task-id
class IdItem;

struct AllocateItem {
    int no;
    int attempt;
    std::string endpoint;
    TaskState state;
    time_t alloc_time;
    time_t period;
};

// For time heap to justify the timestamp
struct AllocateItemComparator {
    bool operator()(AllocateItem* const& litem, AllocateItem* const& ritem) const {
        return litem->alloc_time > ritem->alloc_time;
    }
};

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
    virtual time_t GetStartTime() const = 0;
    virtual time_t GetFinishTime() const = 0;
    virtual TaskStatistics GetStatistics() const = 0;
    virtual Status GetHistory(const std::vector& buf) const = 0;

    // For backup and recovery
    // Load()
    // Dump()
};

class AlphaGru : public Gru<ResourceItem> {
public:
    // Operations
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    // Data getters
    virtual time_t GetStartTime() const;
    virtual time_t GetFinishTime() const;
    virtual TaskStatistics GetStatistics() const;
    virtual Status GetHistory(const std::vector& buf) const;

};

class BetaGru : public Gru<IdItem> {
public:
    // Operations
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    // Data getters
    virtual time_t GetStartTime() const;
    virtual time_t GetFinishTime() const;
    virtual TaskStatistics GetStatistics() const;
    virtual Status GetHistory(const std::vector& buf) const;
};

class OmegaGru : public Gru<IdItem> {
public:
    // Operations
    virtual Status Start();
    virtual Status Update(const std::string& priority, int capacity);
    virtual Status Kill();
    virtual Resource* Assign(const std::string& endpoint, Status* status);
    virtual Status Finish(int no, int attempt, TaskState state);

    // Data getters
    virtual time_t GetStartTime() const;
    virtual time_t GetFinishTime() const;
    virtual TaskStatistics GetStatistics() const;
    virtual Status GetHistory(const std::vector& buf) const;
};

}
}

#endif

