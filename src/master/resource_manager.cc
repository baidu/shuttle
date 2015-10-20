#include "resource_manager.h"
#include "logging.h"
#include <gflags/gflags.h>

DECLARE_int32(input_block_size);

namespace baidu {
namespace shuttle {

BasicManager::BasicManager(int n) {
    for (int i = 0; i < n; ++i) {
        IdItem* item = new IdItem();
        item->no = i;
        item->attempt = 0;
        resource_pool_.push_back(item);
        pending_res_.push_back(item);
    }
}

BasicManager::~BasicManager() {
    MutexLock lock(&mu_);
    for (std::vector<IdItem*>::iterator it = resource_pool_.begin();
            it != resource_pool_.end(); ++it) {
        delete *it;
    }
}

IdItem* BasicManager::GetItem() {
    MutexLock lock(&mu_);
    if (pending_res_.empty()) {
        return NULL;
    }
    IdItem* cur = pending_res_.front();
    cur->attempt ++;
    pending_res_.pop_front();
    running_res_.push_back(cur);
    return new IdItem(*cur);
}

IdItem* BasicManager::GetCertainItem(int no) {
    MutexLock lock(&mu_);
    std::list<IdItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it == running_res_.end()) {
        LOG(WARNING, "this resource has not been allocated: %d", no);
        return NULL;
    }
    (*it)->attempt ++;
    return new IdItem(*(*it));
}

void BasicManager::ReturnBackItem(int no) {
    MutexLock lock(&mu_);
    std::list<IdItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it != running_res_.end()) {
        IdItem* cur = *it;
        running_res_.erase(it);
        pending_res_.push_front(cur);
    } else {
        LOG(WARNING, "invalid resource: %d", no);
    }
}

bool BasicManager::FinishItem(int no) {
    MutexLock lock(&mu_);
    std::list<IdItem*>::iterator it;
    for (it = running_res_.begin(); it != running_res_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it != running_res_.end()) {
        running_res_.erase(it);
        return true;
    } else {
        LOG(WARNING, "resource may have been finished: %d", no);
        return false;
    }
}

IdItem* const BasicManager::CheckCertainItem(int no) {
    MutexLock lock(&mu_);
    std::vector<IdItem*>::iterator it;
    for (it = resource_pool_.begin(); it != resource_pool_.end(); ++it) {
        if ((*it)->no == no) {
            break;
        }
    }
    if (it != resource_pool_.end()) {
        return *it;
    }
    LOG(WARNING, "resource inexist: %d", no);
    return NULL;
}

ResourceManager::ResourceManager(const std::vector<std::string>& input_files) {
    dfs_ = new DfsAdaptor();
    std::vector<FileInfo> files;
    dfs_->Connect(DfsAdaptor::GetServerFromPath(input_files[0]));
    for (std::vector<std::string>::const_iterator it = input_files.begin();
            it != input_files.end(); ++it) {
        if (it->find('*') == std::string::npos) {
            dfs_->ListDirectory(*it, files);
        } else {
            dfs_->GlobDirectory(*it, files);
        }
    }
    MutexLock lock(&mu_);
    int counter = resource_pool_.size();
    const int64_t block_size = FLAGS_input_block_size;
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
            resource_pool_.push_back(item);
        }
        int rest = it->size - blocks * block_size;
        ResourceItem* item = new ResourceItem();
        item->no = counter++;
        item->attempt = 0;
        item->input_file = it->name;
        item->offset = blocks * block_size;
        item->size = rest;
        resource_pool_.push_back(item);
    }
    manager_ = new BasicManager(resource_pool_.size());
}

ResourceManager::~ResourceManager() {
    MutexLock lock(&mu_);
    for (std::vector<ResourceItem*>::iterator it = resource_pool_.begin();
            it != resource_pool_.end(); ++it) {
        delete *it;
    }
    delete manager_;
    delete dfs_;
}

ResourceItem* ResourceManager::GetItem() {
    IdItem* item = manager_->GetItem();
    if (item == NULL) {
        return NULL;
    }
    ResourceItem* resource = resource_pool_[item->no];
    resource->attempt = item->attempt;
    delete item;
    return new ResourceItem(*resource);
}

ResourceItem* ResourceManager::GetCertainItem(int no) {
    IdItem* item = manager_->GetCertainItem(no);
    if (item == NULL) {
        return NULL;
    }
    ResourceItem* resource = resource_pool_[no];
    resource->attempt = item->attempt;
    delete item;
    return new ResourceItem(*resource);
}

void ResourceManager::ReturnBackItem(int no) {
    manager_->ReturnBackItem(no);
}

bool ResourceManager::FinishItem(int no) {
    return manager_->FinishItem(no);
}

ResourceItem* const ResourceManager::CheckCertainItem(int no) {
    IdItem* const item = manager_->CheckCertainItem(no);
    return resource_pool_[item->no];
}

}
}

