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

struct ResourceItem {
    int no;
    int attempt;
    std::string input_file;
    int64_t offset;
    int64_t size;
};

class ResourceManager {
public:
    ResourceManager();
    ResourceManager(const std::string& dfs_server);
    virtual ~ResourceManager();

    void SetInputFiles(const std::vector<std::string>& input_files);

    ResourceItem* GetItem();
    ResourceItem* GetCertainItem(int no);
    void ReturnBackItem(int no);
    void FinishItem(int no);

    ResourceItem* const CheckCertainItem(int no);

    int SumOfItem() {
        MutexLock lock(&mu_);
        return resource_pool_.size();
    }
private:
    Mutex mu_;
    DfsAdaptor* dfs_;
    std::vector<ResourceItem*> resource_pool_;
    std::deque<ResourceItem*> pending_res_;
    std::list<ResourceItem*> running_res_;
};

}
}

#endif

