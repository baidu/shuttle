#include "resource_manager.h"
#include "logging.h"
#include <gflags/gflags.h>

DECLARE_int32(input_block_size);

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
    std::vector<FileInfo> files;
    for (std::vector<std::string>::const_iterator it = input_files.begin();
            it != input_files.end(); ++it) {
        dfs_->ListDirectory(*it, files);
    }
    // TODO build resource table
    MutexLock lock(&mu_);
    int counter = 0;
    const int block_size = FLAGS_input_block_size;
    for (std::vector<FileInfo>::iterator it = files.begin();
            it != files.end(); ++it) {
        int blocks = it->size / block_size;
        for (int i = 0; i < blocks; ++i) {
            ResourceItem* item = new ResourceItem();
            item->no = counter++;
            item->attempt = 0;
            item->input_file = it->name;
            item->offset = i * block_size;
            item->size = block_size;
            item->state = kTaskPending;
            resource_pool_.push_back(item);
            pending_res_.push_back(item);
        }
        int rest = it->size - blocks * block_size;
        ResourceItem* item = new ResourceItem();
        item->no = counter++;
        item->attempt = 0;
        item->input_file = it->name;
        item->offset = blocks * block_size;
        item->size = rest;
        item->state = kTaskPending;
        resource_pool_.push_back(item);
        pending_res_.push_back(item);
    }
}

ResourceItem* ResourceManager::GetItem(const std::string& endpoint) {
    // TODO Replica
    MutexLock lock(&mu_);
    ResourceItem* cur = pending_res_.front();
    pending_res_.pop_front();
    running_res_.insert(cur);
    cur->state = kTaskRunning;
    cur->endpoint = endpoint;
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

void ResourceManager::SetState(int no, int attempt, TaskState state) {
    MutexLock lock(&mu_);
    std::set<ResourceItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no && (*it)->attempt == attempt) {
            break;
        }
    }
    if (it != running_res_.end()) {
        (*it)->state = state;
    } else {
        LOG(WARNING, "invalid resource pair:< no - %d, attempt - %d >", no, attempt);
    }
}

}
}

