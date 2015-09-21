#include "resource_manager.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

ResourceManager::ResourceManager(const std::string& dfs_server) {
    dfs_ = new DfsAdaptor(dfs_server);
}

ResourceManager::~ResourceManager() {
    MutexLock lock(&mu_);
    for (std::vector<ResourceItem*>::iterator it = resource_pool_.begin();
            it != resource_pool_.end(); ++it) {
        delete *it;
    }
    delete dfs_;
}

void ResourceManager::SetInputFiles(const std::vector<std::string>& input_files) {
    int num = input_files.size();
    std::vector<FileInfo> files;
    for (int i = 0; i < num; ++i) {
        dfs_->ListDirectory(input_files[i], files);
    }
    // TODO build resource table
    MutexLock lock(&mu_);
}

ResourceItem* ResourceManager::GetItem() {
    MutexLock lock(&mu_);
    ResourceItem* cur = pending_res_.front();
    pending_res_.pop_front();
    running_res_.insert(cur);
    return new ResourceItem(*cur);
}

void ResourceManager::ReturnBackItem(int no, int attempt) {
    MutexLock lock(&mu_);
    std::set<ResourceItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no && (*it)->attempt == attempt) {
            break;
        }
    }
    if (it != running_res_.end()) {
        ResourceItem* cur = *it;
        running_res_.erase(it);
        pending_res_.push_front(cur);
    } else {
        LOG(WARNING, "invalid resource pair:< no - %d, attempt - %d >", no, attempt);
    }
}

}
}

