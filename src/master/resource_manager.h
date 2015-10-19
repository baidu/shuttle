#ifndef _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#define _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#include <vector>
#include <deque>
#include <list>
#include <string>
#include <stdint.h>

#include "proto/shuttle.pb.h"
#include "common/dfs_adaptor.h"
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

class BasicManager {
public:
    BasicManager(int n);
    virtual ~BasicManager();

    virtual IdItem* GetItem();
    virtual IdItem* GetCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    IdItem* const CheckCertainItem(int no);

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

class ResourceManager {
public:
    ResourceManager(const std::vector<std::string>& input_files);
    virtual ~ResourceManager();

    virtual ResourceItem* GetItem();
    virtual ResourceItem* GetCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    ResourceItem* const CheckCertainItem(int no);

    int SumOfItem() {
        MutexLock lock(&mu_);
        return resource_pool_.size();
    }

private:
    Mutex mu_;
    DfsAdaptor* dfs_;
    std::vector<ResourceItem*> resource_pool_;
    BasicManager* manager_;
};

}
}

#endif

