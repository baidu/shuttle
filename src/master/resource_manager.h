#ifndef _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#define _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#include <vector>
#include <deque>
#include <string>

#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {
    
enum ResourceType {
    kIdItem = 0,
    kFileItem = 1
};

enum ResourceStatus {
    kResPending = 0,
    kResAllocated = 1,
    kResDone = 2
};

class ResourceItem {
public:
    ResourceType type;
    int no;
    int attempt;
    ResourceStatus status;
    std::string input_file;
    int64_t offset;
    int64_t size;
    int allocated;
};

class ResourceManager {
public:
    // Manage resource items
    virtual ResourceItem* GetItem() = 0;
    virtual ResourceItem* GetCertainItem(int no) = 0;
    virtual ResourceItem* CheckCertainItem(int no) = 0;
    virtual void ReturnBackItem(int no) = 0;

    // Status query
    virtual bool FinishItem(int no) = 0;
    virtual bool IsAllocated(int no) = 0;
    virtual bool IsDone(int no) = 0;

    // Statistics interfaces
    virtual int SumOfItem() = 0;
    virtual int Pending() = 0;
    virtual int Allocated() = 0;
    virtual int Done() = 0;

    // For backup and recovery
    virtual void Load(const std::vector<ResourceItem>& data) = 0;
    virtual std::vector<ResourceItem> Dump() = 0;
    virtual ~ResourceManager() { }

    // Factory methods
    static ResourceManager* GetIdManager(int n);
    static ResourceManager* GetBlockManager(
            std::vector<DfsInfo>& inputs, int64_t split_size);
    static ResourceManager* GetNLineManager(std::vector<DfsInfo>& inputs);
    static ResourceManager* BuildManagerFromBackup(
            const std::vector<ResourceItem>& data);
};

}
}

#endif

