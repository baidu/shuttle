#include "resource_manager.h"
#include "logging.h"
#include <gflags/gflags.h>

DECLARE_int32(input_block_size);

namespace baidu {
namespace shuttle {

ResourceManager::ResourceManager() {
    dfs_ = new DfsAdaptor();
}

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
    std::vector<FileInfo> files;
    std::string last_dfs_server;
    for (std::vector<std::string>::const_iterator it = input_files.begin();
            it != input_files.end(); ++it) {
        const std::string& cur_server = DfsAdaptor::GetServerFromPath(*it);
        if (cur_server != last_dfs_server) {
            dfs_->Disconnect();
            // May need to check return values
            dfs_->Connect(cur_server);
            last_dfs_server = cur_server;
        }
        dfs_->ListDirectory(*it, files);
    }
    MutexLock lock(&mu_);
    int counter = 0;
    const int block_size = FLAGS_input_block_size;
    for (std::vector<FileInfo>::iterator it = files.begin();
            it != files.end(); ++it) {
        int blocks = it->size / block_size;
        for (int i = 0; i < blocks; ++i) {
            ResourceItem* item = new ResourceItem();
            item->no = counter++;
            item->input_file = it->name;
            item->offset = i * block_size;
            item->size = block_size;
            resource_pool_.push_back(item);
            pending_res_.push_back(item);
        }
        int rest = it->size - blocks * block_size;
        ResourceItem* item = new ResourceItem();
        item->no = counter++;
        item->input_file = it->name;
        item->offset = blocks * block_size;
        item->size = rest;
        resource_pool_.push_back(item);
        pending_res_.push_back(item);
    }
}

ResourceItem* ResourceManager::GetItem() {
    MutexLock lock(&mu_);
    ResourceItem* cur = pending_res_.front();
    pending_res_.pop_front();
    running_res_.push_back(cur);
    return new ResourceItem(*cur);
}

ResourceItem* ResourceManager::GetCertainItem(int no) {
    MutexLock lock(&mu_);
    std::list<ResourceItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it != running_res_.end()) {
        return new ResourceItem(*(*it));
    } else {
        LOG(WARNING, "this resource has not been allocated: %d", no);
    }
    return NULL;
}

void ResourceManager::ReturnBackItem(int no) {
    MutexLock lock(&mu_);
    std::list<ResourceItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it != running_res_.end()) {
        ResourceItem* cur = *it;
        running_res_.erase(it);
        pending_res_.push_front(cur);
    } else {
        LOG(WARNING, "invalid resource: %d", no);
    }
}

void ResourceManager::FinishItem(int no) {
    MutexLock lock(&mu_);
    std::list<ResourceItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it != running_res_.end()) {
        running_res_.erase(it);
    } else {
        LOG(WARNING, "resource may have been finished: %d", no);
    }
}

}
}

