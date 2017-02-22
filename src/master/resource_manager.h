#ifndef _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#define _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#include <vector>
#include <deque>
#include <list>
#include <string>
#include <stdint.h>
#include <map>
#include <boost/shared_ptr.hpp>

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

class ResourceItem;

class IdItem {
public:
    int no;
    int attempt;
    ResourceStatus status;
    int allocated;
    IdItem() : no(0), attempt(0), status(kResPending), allocated(0) { }
    IdItem(const IdItem& res);
    IdItem* operator=(const IdItem& res);
    IdItem* CopyFrom(const IdItem& res);
};

class ResourceItem : public IdItem {
public:
    /* Public member inherited from ResourceManager
     *
     * int no;
     * int attempt;
     * ResourceStatus status;
     * int allocated;
     */
    std::string input_file;
    int64_t offset;
    int64_t size;
    ResourceItem* operator=(const ResourceItem& res) {
        no = res.no;
        attempt = res.attempt;
        status = res.status;
        allocated = res.allocated;
        input_file = res.input_file;
        offset = res.offset;
        size = res.size;
        return this;
    }
    ResourceItem* operator=(const IdItem& id) {
        return CopyFrom(id);
    }
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
    virtual void Load(const std::vector<Resource>& data) = 0;
    virtual std::vector<Resource> Dump() = 0;
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
    virtual void Load(const std::vector<IdItem>& data);
    virtual std::vector<IdItem> Dump();

protected:
    Mutex mu_;
    std::vector<IdItem*> resource_pool_;
    std::deque<IdItem*> pending_res_;
    int pending_;
    int allocated_;
    int done_;
};

class MultiFs {
public:
    FileSystem* GetFs(const std::string& file_path, FileSystem::Param param);
private:
    std::map<std::string, boost::shared_ptr<FileSystem> > fs_map_;
    Mutex mu_;
};

class ResourceManager : public BasicResourceManager<ResourceItem> {
public:
    ResourceManager(const std::vector<std::string>& input_files,
                    FileSystem::Param& param, int64_t split_size);
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
    virtual void Load(const std::vector<ResourceItem>& data);
    virtual void Load(const std::vector<IdItem>& data);
    virtual std::vector<ResourceItem> Dump();

protected:
    ResourceManager() : manager_(NULL) { }

protected:
    Mutex mu_;
    std::vector<ResourceItem*> resource_pool_;
    IdManager* manager_;
    MultiFs multi_fs_;

private:
    void ExpandWildcard(const std::vector<std::string>& input_files,
                        std::vector<std::string>& expand_files,
                        FileSystem::Param& param);
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

