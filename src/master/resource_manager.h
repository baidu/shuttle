#ifndef _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#define _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#include <vector>
#include <deque>
#include <string>

#include "common/filesystem.h"

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
//    ResourceItem() { }
//    ResourceItem(const ResourceItem& res);
//    ResourceItem* operator=(const ResourceItem& res);
//    ResourceItem* CopyFrom(const ResourceItem& res);
};

class ResourceManager {
public:
    virtual ResourceItem* GetItem() = 0;
    virtual ResourceItem* GetCertainItem(int no) = 0;
    virtual ResourceItem* CheckCertainItem(int no) = 0;
    virtual void ReturnBackItem(int no) = 0;
    virtual bool FinishItem(int no) = 0;
    virtual bool IsAllocated(int no) = 0;
    virtual bool IsDone(int no) = 0;
    virtual int SumOfItem() = 0;
    virtual int Pending() = 0;
    virtual int Allocated() = 0;
    virtual int Done() = 0;
    virtual void Load(const std::vector<ResourceItem>& data) = 0;
    virtual std::vector<ResourceItem> Dump() = 0;

    static ResourceManager* GetIdManager(int n);
    static ResourceManager* GetBlockManager(
            const std::vector<std::string>& input_files,
            FileSystem::Param& param, int64_t split_size);
    static ResourceManager* GetNLineManager(
            const std::vector<std::string>& input_files, FileSystem::Param& param);
};

}
}

#endif

