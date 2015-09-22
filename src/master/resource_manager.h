#ifndef _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#define _BAIDU_SHUTTLE_RESOURCE_MANAGER_H_
#include <vector>
#include <deque>
#include <set>
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
    TaskState state;
    std::string endpoint;

};

class ResourceManager {
public:
    ResourceManager(const std::string& dfs_server);
    virtual ~ResourceManager();

    void SetInputFiles(const std::vector<std::string>& input_files);

    ResourceItem* GetItem(const std::string& endpoint);
    void ReturnBackItem(int no, int attempt);
    void SetState(int no, int attempt, TaskState state);

private:
    Mutex mu_;
    DfsAdaptor* dfs_;
    std::vector<ResourceItem*> resource_pool_;
    std::deque<ResourceItem*> pending_res_;
    std::set<ResourceItem*> running_res_;
};

}
}

#endif

