#include "resource_manager.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include "logging.h"
#include "thread_pool.h"
#include "mutex.h"
#include "common/file.h"
#include "common/scanner.h"

DECLARE_int32(input_block_size);
DECLARE_int32(parallel_attempts);

namespace baidu {
namespace shuttle {

// Used in listing directory. Determine the size of thread pool
static const int parallel_level = 10;

class ResourceManagerImpl : public ResourceManager {
public:
    // This class is abstract level for common operations in different manager
    //   Constructor is only used for recovering from backup
    ResourceManagerImpl() : pending_(0), allocated_(0), done_(0) { }
    virtual ~ResourceManagerImpl();

    virtual ResourceItem* GetItem();
    virtual ResourceItem* GetCertainItem(int no);
    virtual ResourceItem* CheckCertainItem(int no);
    virtual void ReturnBackItem(int no);
    virtual bool FinishItem(int no);

    virtual bool IsAllocated(int no);
    virtual bool IsDone(int no);

    virtual int SumOfItems() {
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
    virtual void Load(const std::vector<ResourceItem>& data);
    virtual std::vector<ResourceItem> Dump();

protected:
    /*
     * All members need a careful initialization due to the absence of constructor
     * Customer constructor should add proper item in resource pool and pending queue,
     *   and set counters properly
     */
    Mutex mu_;
    std::vector<ResourceItem*> resource_pool_;
    std::deque<ResourceItem*> pending_res_;
    int pending_;
    int allocated_;
    int done_;
};

// Stores id information, not related with input files
class IdManager : public ResourceManagerImpl {
public:
    IdManager(int size);
    virtual ~IdManager() { }
};

// Stores information of block-splited files and ids
class BlockManager : public ResourceManagerImpl {
public:
    BlockManager(std::vector<DfsInfo>& inputs, int64_t split_size);
    virtual ~BlockManager() { }
};

// Stores information of line-splited files and ids
class NLineManager : public ResourceManagerImpl {
public:
    NLineManager(std::vector<DfsInfo>& inputs);
    virtual ~NLineManager() { }
};

// Factory Functions

ResourceManager* ResourceManager::GetIdManager(int size) {
    return new IdManager(size);
}

ResourceManager* ResourceManager::GetBlockManager(
        std::vector<DfsInfo>& inputs, int64_t split_size) {
    return new BlockManager(inputs, split_size);
}

ResourceManager* ResourceManager::GetNLineManager(std::vector<DfsInfo>& inputs) {
    return new NLineManager(inputs);
}

ResourceManager* ResourceManager::BuildManagerFromBackup(
        const std::vector<ResourceItem>& data) {
    if (data.empty()) {
        return NULL;
    }
    ResourceManager* ret = new ResourceManagerImpl();
    ret->Load(data);
    return ret;
}

// Implementations here

ResourceManagerImpl::~ResourceManagerImpl() {
    MutexLock lock(&mu_);
    for (std::vector<ResourceItem*>::iterator it = resource_pool_.begin();
            it != resource_pool_.end(); ++it) {
        delete *it;
    }
}

ResourceItem* ResourceManagerImpl::GetItem() {
    MutexLock lock(&mu_);
    while (!pending_res_.empty() && pending_res_.front()->status != kResPending) {
        pending_res_.pop_front();
    }
    if (pending_res_.empty()) {
        return NULL;
    }
    ResourceItem* cur = pending_res_.front();
    pending_res_.pop_front();
    cur->attempt ++;
    cur->status = kResAllocated;
    cur->allocated ++;
    -- pending_; ++ allocated_;
    return new ResourceItem(*cur);
}

ResourceItem* ResourceManagerImpl::GetCertainItem(int no) {
    size_t n = static_cast<size_t>(no);
    MutexLock lock(&mu_);
    if (n > resource_pool_.size()) {
        LOG(WARNING, "this resource is not valid for duplication: %d", no);
        return NULL;
    }
    ResourceItem* cur = resource_pool_[n];
    if (cur->allocated > FLAGS_parallel_attempts) {
        LOG(INFO, "resource distribution has reached limitation: %d", cur->no);
        return NULL;
    }
    if (cur->status == kResPending) {
        cur->status = kResAllocated;
        -- pending_; ++ allocated_;
    }
    if (cur->status == kResAllocated) {
        cur->attempt ++;
        cur->allocated ++;
        return new ResourceItem(*cur);
    }
    if (cur->status == kResDone) {
        LOG(INFO, "this resource has been done: %d", no);
    } else {
        LOG(WARNING, "this resource has not been allocated: %d", no);
    }
    return NULL;
}

ResourceItem* ResourceManagerImpl::CheckCertainItem(int no) {
    size_t n = static_cast<size_t>(no);
    MutexLock lock(&mu_);
    if (n > resource_pool_.size()) {
        LOG(WARNING, "this resource is not valid for checking: %d", no);
        return NULL;
    }
    return new ResourceItem(*(resource_pool_[n]));
}

void ResourceManagerImpl::ReturnBackItem(int no) {
    size_t n = static_cast<size_t>(no);
    MutexLock lock(&mu_);
    if (n > resource_pool_.size()) {
        LOG(WARNING, "this resource is not valid for returning: %d", no);
        return;
    }
    ResourceItem* cur = resource_pool_[n];
    if (cur->status == kResAllocated) {
        if (-- cur->allocated <= 0) {
            cur->status = kResPending;
            pending_res_.push_front(cur);
            -- allocated_; ++ pending_;
        }
    } else {
        LOG(WARNING, "invalid resource: %d", no);
    }
}

bool ResourceManagerImpl::FinishItem(int no) {
    size_t n = static_cast<size_t>(no);
    MutexLock lock(&mu_);
    if (n > resource_pool_.size()) {
        LOG(WARNING, "this resource is not valid for finishing: %d", no);
        return false;
    }
    ResourceItem* cur = resource_pool_[n];
    if (cur->status == kResAllocated) {
        cur->status = kResDone;
        cur->allocated = 0;
        -- allocated_; ++ done_;
        return true;
    }
    LOG(WARNING, "resource may have been finished: %d", no);
    return false;
}

bool ResourceManagerImpl::IsAllocated(int no) {
    size_t n = static_cast<size_t>(no);
    MutexLock lock(&mu_);
    if (n > resource_pool_.size()) {
        LOG(WARNING, "this resource is not valid for checking allocated: %d", no);
        return false;
    }
    return resource_pool_[n]->status == kResAllocated;
}

bool ResourceManagerImpl::IsDone(int no) {
    size_t n = static_cast<size_t>(no);
    MutexLock lock(&mu_);
    if (n > resource_pool_.size()) {
        LOG(WARNING, "this resource is not valid for checking done: %d", no);
        return false;
    }
    return resource_pool_[n]->status == kResDone;
}

void ResourceManagerImpl::Load(const std::vector<ResourceItem>& data) {
    if (data.size() > resource_pool_.size()) {
        LOG(INFO, "data size differs in resource manager loading, resize");
        resource_pool_.resize(data.size(), NULL);
    }
    std::vector<ResourceItem*>::iterator dst = resource_pool_.begin();
    for (std::vector<ResourceItem>::const_iterator src = data.begin();
            src != data.end(); ++src,++dst) {
        if (*dst == NULL) {
            *dst = new ResourceItem();
        }
        *(*dst) = *src;
    }
    pending_ = 0;
    allocated_ = 0;
    done_ = 0;
    pending_res_.clear();
    for (std::vector<ResourceItem*>::iterator it = resource_pool_.begin();
            it != resource_pool_.end(); ++it) {
        switch((*it)->status) {
        case kResPending:
            ++ pending_;
            pending_res_.push_back(*it);
            break;
        case kResAllocated: ++ allocated_; break;
        case kResDone: ++ done_; break;
        default: break;
        }
    }
}

std::vector<ResourceItem> ResourceManagerImpl::Dump() {
    std::vector<ResourceItem> copy;
    MutexLock lock(&mu_);
    for (std::vector<ResourceItem*>::iterator it = resource_pool_.begin();
            it != resource_pool_.end(); ++it) {
        copy.push_back(*(*it));
    }
    return copy;
}

IdManager::IdManager(int size) {
    pending_ = size;
    for (int i = 0; i < size; ++i) {
        ResourceItem* item = new ResourceItem();
        item->no = i;
        item->attempt = 0;
        item->status = kResPending;
        item->allocated = 0;
        resource_pool_.push_back(item);
        pending_res_.push_back(item);
    }
}

BlockManager::BlockManager(std::vector<DfsInfo>& inputs, int64_t split_size) {
    if (inputs.size() == 0) {
        return;
    }
    FileHub<File>* hub = FileHub<File>::GetHub();
    for (std::vector<DfsInfo>::iterator it = inputs.begin(); it != inputs.end(); ++it) {
        const File::Param& param = File::BuildParam(*it);
        std::string host = param.find("host") == param.end() ? "" : param.find("host")->second;
        std::string port = param.find("port") == param.end() ? "" : param.find("port")->second;
        if (hub->Get(host, port) != NULL) {
            continue;
        }
        File* fp = File::Create(kInfHdfs, param);
        if (fp == NULL) {
            LOG(WARNING, "cannot build file pointer, param size: %d", param.size());
            continue;
        }
        if (hub->Store(param, fp) != fp) {
            LOG(WARNING, "fail to store in hub: %s", it->path().c_str());
        }
    }

    std::vector<FileInfo> files;
    int i = 0;
    std::vector<std::string> expand_input_files;
    // Preprocess middle wildcards to accelerate parallel listing
    for (size_t i = 0; i < inputs.size(); i++) {
        LOG(INFO, "expanding: %s", inputs[i].path().c_str());
        const std::string& file_name = inputs[i].path();
        size_t prefix_pos ;
        if ((prefix_pos = file_name.find("/*/")) != std::string::npos) {
            const std::string& prefix = file_name.substr(0, prefix_pos);
            const std::string& suffix = file_name.substr(prefix_pos + 3);
            std::vector<FileInfo> children;
            File* fp = hub->Get(inputs[i].host(), inputs[i].port());
            bool ok = fp != NULL && fp->List(prefix, &children);
            if (!ok) {
                expand_input_files.push_back(file_name);
            } else {
                for (size_t j = 0; j < children.size(); j++) {
                    expand_input_files.push_back(children[j].name + "/" + suffix);
                }
            }
        } else {
            expand_input_files.push_back(file_name);
        }
    }
    ::baidu::common::ThreadPool tp(parallel_level);
    std::vector<FileInfo> sub_files[parallel_level];
    // Use thread pool to list directory concurrently
    for (std::vector<std::string>::const_iterator it = expand_input_files.begin();
            it != expand_input_files.end(); ++it) {
        LOG(DEBUG, "input file: %s", it->c_str());
        std::string path;
        if (!File::ParseFullAddress(*it, NULL, NULL, NULL, &path)) {
			path = *it;
		}
        File* fp = hub->Get(*it);
        if (fp == NULL) {
            LOG(WARNING, "got empty file pointer: %s", it->c_str());
            continue;
        }
        if (path.find('*') == std::string::npos) {
            tp.AddTask(boost::bind(&File::List, fp, path, &sub_files[i]));
        } else {
            tp.AddTask(boost::bind(&File::Glob, fp, path, &sub_files[i]));
        }
        i = (i + 1) % parallel_level;
    }
    tp.Stop(true);
    delete hub;
    // Merge all results
    for (int i = 0; i < parallel_level; ++i) {
        files.reserve(files.size() + sub_files[i].size());
        files.insert(files.end(), sub_files[i].begin(), sub_files[i].end());
    }
    int counter = 0;
    const int64_t block_size = split_size == 0 ? FLAGS_input_block_size : split_size;
    for (std::vector<FileInfo>::iterator it = files.begin();
            it != files.end(); ++it) {
        int blocks = it->size / block_size;
        for (int i = 0; i < blocks; ++i) {
            ResourceItem* item = new ResourceItem();
            item->no = counter++;
            item->attempt = 0;
            item->status = kResPending;
            item->allocated = 0;
            item->input_file = it->name;
            item->offset = i * block_size;
            item->size = block_size;
            resource_pool_.push_back(item);
            pending_res_.push_back(item);
        }
        int rest = it->size - blocks * block_size;
        ResourceItem* item = new ResourceItem();
        item->no = counter++;
        item->attempt = 0;
        item->status = kResPending;
        item->allocated = 0;
        item->input_file = it->name;
        item->offset = blocks * block_size;
        item->size = rest;
        resource_pool_.push_back(item);
        pending_res_.push_back(item);
    }
    pending_ = static_cast<int>(resource_pool_.size());
}

NLineManager::NLineManager(std::vector<DfsInfo>& inputs) {
    FileHub<File>* hub = FileHub<File>::GetHub();
    std::vector<FileInfo> files;
    std::string path;
    for (std::vector<DfsInfo>::iterator it = inputs.begin();
            it != inputs.end(); ++it) {
        const File::Param& param = File::BuildParam(*it);
        std::string host = param.find("host") == param.end() ? "" : param.find("host")->second;
        std::string port = param.find("port") == param.end() ? "" : param.find("port")->second;
        File* fp = NULL;
        if (hub->Get(host, port) == NULL) {
            fp = File::Create(kInfHdfs, param);
            if (fp == NULL) {
                LOG(WARNING, "cannot build file pointer, param size: %d", param.size());
                continue;
            }
            if (hub->Store(param, fp) != fp) {
                LOG(WARNING, "fail to store in hub: %s", it->path().c_str());
                continue;
            }
        }
        std::string path;
        if (!File::ParseFullAddress(it->path(), NULL, NULL, NULL, &path)) {
            path = it->path();
        }
        if (path.find('*') == std::string::npos) {
            hub->Get(host, port)->List(path, &files);
        } else {
            hub->Get(host, port)->Glob(path, &files);
        }
    }
    int counter = 0;
    for (std::vector<FileInfo>::iterator it = files.begin();
            it != files.end(); ++it) {
        std::string path;
        if (!File::ParseFullAddress(it->name, NULL, NULL, NULL, &path)) {
			path = it->name;
		}
        FormattedFile* fp = FormattedFile::Get(hub->Get(it->name), kPlainText);
        if (fp == NULL) {
            LOG(WARNING, "fail to get formatted file: %s", it->name.c_str());
            continue;
        }
        if (!fp->Open(path, kReadFile, hub->GetParam(it->name))) {
            LOG(WARNING, "cannot open formatted file: %s", it->name.c_str());
            delete fp;
            continue;
        }
        Scanner* scanner = Scanner::Get(fp, kInputScanner);
        Scanner::Iterator* read_it = scanner->Scan(0, Scanner::SCAN_MAX_LEN);
        int64_t offset = 0;
        for (; !read_it->Done(); read_it->Next()) {
            // Line-based file format stores line in value
            const std::string& line = read_it->Value();
            ResourceItem* item = new ResourceItem();
            item->no = counter++;
            item->attempt = 0;
            item->status = kResPending;
            item->allocated = 0;
            item->input_file = it->name;
            item->offset = offset;
            item->size = line.size() + 1;
            offset += item->size;
            resource_pool_.push_back(item);
            pending_res_.push_back(item);
        }
        delete read_it;
        delete scanner;
    }
    delete hub;
    pending_ = static_cast<int>(resource_pool_.size());
}

}
}

