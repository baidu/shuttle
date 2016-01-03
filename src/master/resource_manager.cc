#include "resource_manager.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <algorithm>
#include <gflags/gflags.h>
#include <assert.h>
#include "logging.h"
#include "thread_pool.h"
#include "mutex.h"
#include "sort/input_reader.h"
#include "common/tools_util.h"

DECLARE_int32(input_block_size);
DECLARE_int32(parallel_attempts);

namespace baidu {
namespace shuttle {

static const int parallel_level = 10;

class ResourceManagerImpl : public ResourceManager {
public:
    virtual ~ResourceManagerImpl();

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
    // Sorry this class is abstract level for common operations in different manager
    ResourceManagerImpl() : pending_(0), allocated_(0), done_(0) { }

protected:
    // All members need a careful initialization due to the absence of constructor
    // Customer constructor should add proper item in resource pool and pending queue,
    //   and set counters properly
    Mutex mu_;
    std::vector<ResourceItem*> resource_pool_;
    std::deque<ResourceItem*> pending_res_;
    int pending_;
    int allocated_;
    int done_;
};

class IdManager : public ResourceManagerImpl {
public:
    IdManager(int size);
    virtual ~IdManager() { }
};

class BlockManager : public ResourceManagerImpl {
public:
    BlockManager(const std::vector<std::string>& input_files,
                 FileSystem::Param& param, int64_t split_size);
    virtual ~BlockManager() { }
};

class NLineManager : public ResourceManagerImpl {
public:
    NLineManager(const std::vector<std::string>& input_files,
                 FileSystem::Param& param);
    virtual ~NLineManager() { }
};

// Factory Functions

ResourceManager* ResourceManager::GetIdManager(int size) {
    return new IdManager(size);
}

ResourceManager* ResourceManager::GetBlockManager(
        const std::vector<std::string>& input_files,
        FileSystem::Param& param, int64_t split_size) {
    return new BlockManager(input_files, param, split_size);
}

ResourceManager* ResourceManager::GetNLineManager(
        const std::vector<std::string>& input_files, FileSystem::Param& param) {
    return new NLineManager(input_files, param);
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
    assert(data.size() == resource_pool_.size());
    std::vector<ResourceItem*>::iterator dst = resource_pool_.begin();
    for (std::vector<ResourceItem>::const_iterator src = data.begin();
            src != data.end(); ++src,++dst) {
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

BlockManager::BlockManager(const std::vector<std::string>& input_files,
                           FileSystem::Param& param,
                           int64_t split_size) {
    if (input_files.size() == 0) {
        return;
    }
    if (boost::starts_with(input_files[0], "hdfs://")) {
        std::string host;
        int port;
        ParseHdfsAddress(input_files[0], &host, &port, NULL);
        param["host"] = host;
        param["port"] = boost::lexical_cast<std::string>(port);
    }
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    std::vector<FileInfo> files;
    ::baidu::common::ThreadPool tp(parallel_level);
    std::vector<FileInfo> sub_files[parallel_level];
    int i = 0;
    std::vector<std::string> expand_input_files;
    for (size_t i = 0; i < input_files.size(); i++) {
        const std::string& file_name = input_files[i];
        size_t prefix_pos ;
        if ( (prefix_pos = file_name.find("/*/")) != std::string::npos) {
            const std::string& prefix = file_name.substr(0, prefix_pos);
            const std::string& suffix = file_name.substr(prefix_pos + 3);
            std::vector<FileInfo> children;
            bool ok = fs->List(prefix, &children);
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
    for (std::vector<std::string>::const_iterator it = expand_input_files.begin();
            it != expand_input_files.end(); ++it) {
        std::string path;
        LOG(INFO, "input file: %s", it->c_str());
        if (boost::starts_with(*it, "hdfs://")) {
            ParseHdfsAddress(*it, NULL, NULL, &path);
        } else {
            path = *it;
        }
        if (path.find('*') == std::string::npos) {
            tp.AddTask(boost::bind(&FileSystem::List, fs, path, &sub_files[i]));
        } else {
            tp.AddTask(boost::bind(&FileSystem::Glob, fs, path, &sub_files[i]));
        }
        i = (i + 1) % parallel_level;
    }
    tp.Stop(true);
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
    delete fs;
}

NLineManager::NLineManager(const std::vector<std::string>& input_files,
                           FileSystem::Param& param) {
    if (boost::starts_with(input_files[0], "hdfs://")) {
        std::string host;
        int port;
        ParseHdfsAddress(input_files[0], &host, &port, NULL);
        param["host"] = host;
        param["port"] = boost::lexical_cast<std::string>(port);
    }
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    std::vector<FileInfo> files;
    std::string path;
    for (std::vector<std::string>::const_iterator it = input_files.begin();
            it != input_files.end(); ++it) {
        if (boost::starts_with(input_files[0], "hdfs://")) {
            ParseHdfsAddress(*it, NULL, NULL, &path);
        } else {
            path = *it;
        }
        if (path.find('*') == std::string::npos) {
            fs->List(path, &files);
        } else {
            fs->Glob(path, &files);
        }
    }
    int counter = 0;
    for (std::vector<FileInfo>::iterator it = files.begin();
            it != files.end(); ++it) {
        std::string path;
        ParseHdfsAddress(it->name, NULL, NULL, &path);
        InputReader* reader = InputReader::CreateHdfsTextReader();
        if (reader->Open(path, param) != kOk) {
            LOG(WARNING, "set n line file error: %s", it->name.c_str());
            continue;
        }
        int64_t offset = 0;
        for (InputReader::Iterator* read_it = reader->Read(0, ((unsigned long)~0l) >> 1);
                !read_it->Done(); read_it->Next()) {
            const std::string& line = read_it->Record();
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
    }
    pending_ = static_cast<int>(resource_pool_.size());
}

}
}

