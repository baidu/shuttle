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

struct IdItem {
    int no;
    int attempt;
};

struct ResourceItem {
    int no;
    int attempt;
    std::string input_file;
    int64_t offset;
    int64_t size;
};

template <class Resource>
class BasicResourceManager {
    virtual Resource* GetItem() = 0;
    virtual Resource* GetCertainItem(int no) = 0;
    virtual void ReturnBackItem(int no) = 0;
    virtual bool FinishItem(int no) = 0;
    virtual Resource* const CheckCertainItem(int no) = 0;
    virtual int SumOfItem() = 0;
};

class IdManager : public BasicResourceManager<IdItem> {
public:
    IdManager(int n);
    virtual ~IdManager();

    virtual IdItem* GetItem();
    virtual IdItem* GetCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    virtual IdItem* const CheckCertainItem(int no);

    virtual int SumOfItem() {
        MutexLock lock(&mu_);
        return resource_pool_.size();
    }

private:
    Mutex mu_;
    std::vector<IdItem*> resource_pool_;
    std::deque<IdItem*> pending_res_;
    std::list<IdItem*> running_res_;
};

class ResourceManager : public BasicResourceManager<ResourceItem> {
public:
    ResourceManager(const std::vector<std::string>& input_files,
                    FileSystem::Param& param);
    virtual ~ResourceManager();

    virtual ResourceItem* GetItem();
    virtual ResourceItem* GetCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    virtual ResourceItem* const CheckCertainItem(int no);

    virtual int SumOfItem() {
        MutexLock lock(&mu_);
        return resource_pool_.size();
    }

private:
    Mutex mu_;
    FileSystem* fs_;
    std::vector<ResourceItem*> resource_pool_;
    IdManager* manager_;
};

}
}

#endif

