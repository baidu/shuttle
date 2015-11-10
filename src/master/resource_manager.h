#ifndef _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#define _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#include <vector>
#include <deque>
#include <list>
#include <string>
#include <stdint.h>

#include "proto/shuttle.pb.h"
#include "common/filesystem.h"
#include "mutex.h"

namespace baidu {
namespace shuttle {
    
enum ResourceStatus {
    kResPending = 0,
    kResAllocated = 1,
    kResDone = 2
};

struct IdItem {
    int no;
    int attempt;
    ResourceStatus status;
    int allocated;
};

struct ResourceItem {
    int no;
    int attempt;
    ResourceStatus status;
    int allocated;
    std::string input_file;
    int64_t offset;
    int64_t size;
    ResourceItem* CopyFrom(const IdItem& id) {
        no = id.no;
        attempt = id.attempt;
        status = id.status;
        allocated = id.allocated;
        return this;
    }
};

template <class Resource>
class BasicResourceManager {
public:
    virtual Resource* GetItem() = 0;
    virtual Resource* GetCertainItem(int no) = 0;
    virtual Resource* CheckCertainItem(int no) = 0;
    virtual void ReturnBackItem(int no) = 0;
    virtual bool FinishItem(int no) = 0;
    virtual bool IsAllocated(int no) = 0;
    virtual bool IsDone(int no) = 0;
    virtual int SumOfItem() = 0;
    virtual int Pending() = 0;
    virtual int Allocated() = 0;
    virtual int Done() = 0;
};

class IdManager : public BasicResourceManager<IdItem> {
public:
    IdManager(int n);
    virtual ~IdManager();

    virtual IdItem* GetItem();
    virtual IdItem* GetCertainItem(int no);
    virtual IdItem* CheckCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    virtual bool IsAllocated(int no);
    virtual bool IsDone(int no);

    virtual int SumOfItem() {
        MutexLock lock(&mu_);
        return resource_pool_.size();
    }
    virtual int Pending() {
        MutexLock lock(&mu_);
        return pending_;
    }
    virtual int Allocated() {
        MutexLock lock(&mu_);
        return allocated_;
    }
    virtual int Done() {
        MutexLock lock(&mu_);
        return done_;
    }

protected:
    Mutex mu_;
    std::vector<IdItem*> resource_pool_;
    std::deque<IdItem*> pending_res_;
    int pending_;
    int allocated_;
    int done_;
};

class ResourceManager : public BasicResourceManager<ResourceItem> {
public:
    ResourceManager(const std::vector<std::string>& input_files,
                    FileSystem::Param& param);
    virtual ~ResourceManager();

    virtual ResourceItem* GetItem();
    virtual ResourceItem* GetCertainItem(int no);
    virtual ResourceItem* CheckCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    virtual bool IsAllocated(int no);
    virtual bool IsDone(int no);

    virtual int SumOfItem() {
        MutexLock lock(&mu_);
        return resource_pool_.size();
    }
    virtual int Pending() {
        MutexLock lock(&mu_);
        return manager_->Pending();
    }
    virtual int Allocated() {
        MutexLock lock(&mu_);
        return manager_->Allocated();
    }
    virtual int Done() {
        MutexLock lock(&mu_);
        return manager_->Done();
    }

protected:
    ResourceManager() : fs_(NULL), manager_(NULL) { }

protected:
    Mutex mu_;
    FileSystem* fs_;
    std::vector<ResourceItem*> resource_pool_;
    IdManager* manager_;
};

class NLineResourceManager : public ResourceManager {
public:
    NLineResourceManager(const std::vector<std::string>& input_files,
                         FileSystem::Param& param);
    virtual ~NLineResourceManager() { }

    /* Public method inherited from ResourceManager
     *
     * virtual ResourceItem* GetItem();
     * virtual ResourceItem* GetCertainItem(int no);
     * virtual void ReturnBackItem(int no);
     * virtual bool FinishItem(int no);

     * virtual ResourceItem* const CheckCertainItem(int no);

     * virtual int SumOfItem();
     */
};

}
}

#endif

