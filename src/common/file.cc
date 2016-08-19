#include "file.h"

#include <deque>
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <fcntl.h> 
#include <stdio.h> 
#include <sys/stat.h> 
#include <sys/types.h> 
#include <unistd.h> 

#include "hdfs.h"
#include "logging.h"
#include "mutex.h"
#include "common/tools_util.h"

namespace baidu {
namespace shuttle {

class InfHdfs : public File {
public:
    InfHdfs();
    virtual ~InfHdfs() { }

    void Connect(const Param& param);
    virtual bool Open(const std::string& path, OpenMode mode);
    virtual bool Close();
    virtual bool Seek(int64_t pos);
    virtual int32_t Read(void* buf, size_t len);
    virtual int32_t Write(void* buf, size_t len);
    virtual int64_t Tell();
    virtual int64_t GetSize();
    virtual bool Rename(const std::string& old_name, const std::string& new_name);
    virtual bool Remove(const std::string& path);
    virtual bool List(const std::string& dir, std::vector<FileInfo>* children);
    virtual bool Glob(const std::string& dir, std::vector<FileInfo>* children);
    virtual bool Mkdirs(const std::string& dir);
    virtual bool Exist(const std::string& path);

    static void ConnectInfHdfs(const Param& param, hdfsFS* fs);
private:
    hdfsFS fs_;
    hdfsFile fd_;
    std::string path_;
};


class LocalFs : public File {
public:
    LocalFs();
    virtual ~LocalFs() { }

    virtual bool Open(const std::string& path, OpenMode mode);
    virtual bool Close();
    virtual bool Seek(int64_t pos);
    virtual int32_t Read(void* buf, size_t len);
    virtual int32_t Write(void* buf, size_t len);
    virtual int64_t Tell();
    virtual int64_t GetSize();
    virtual bool Rename(const std::string& old_name, const std::string& new_name);
    virtual bool Remove(const std::string& path);
    virtual bool List(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        //TODO, not implementation
        return false;
    }
    virtual bool Glob(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        //TODO, not implementation
        return false;
    }
    virtual bool Mkdirs(const std::string& dir);
    virtual bool Exist(const std::string& path);
private:
    int fd_;
    std::string path_;
};

class FileHubImpl : public FileHub {
public:
    FileHubImpl() { }
    virtual ~FileHubImpl() { }

    virtual File* BuildFs(DfsInfo& info);
    virtual File* GetFs(const std::string& address);
    virtual File::Param GetParam(const std::string& address);

private:
    std::map< std::string, boost::shared_ptr<File> > fs_map_;
    std::map< std::string, File::Param > param_map_;
    Mutex mu_;
};

File* File::Create(FileType type, const Param& param) {
    switch(type) {
    case kLocalFs:
        return new LocalFs();
    case kInfHdfs:
        InfHdfs* fs = new InfHdfs();
        fs->Connect(param);
        return fs;
    }
    return NULL;
}

bool File::WriteAll(void* buf, size_t len) {
    size_t start = 0;
    char* str = (char*)buf;
    while (start < len) {
        int write_bytes = Write(&str[start], len - start);
        if ( write_bytes < 0){
            return false;
        }
        start += write_bytes;
    }
    return true;
}

InfHdfs::InfHdfs() : fs_(NULL), fd_(NULL) {

}

void InfHdfs::ConnectInfHdfs(const Param& param, hdfsFS* fs) {
    if (param.find("user") != param.end()) {
        const std::string& user = param["user"];
        const std::string& password = param["password"];
        const std::string& host = param["host"];
        const std::string& port = param["port"];
        *fs = hdfsConnectAsUser(host.c_str(), atoi(port.c_str()),
                                user.c_str(),
                                password.c_str());
        LOG(INFO, "hdfsConnectAsUser: %s:%d, %s", host.c_str(),
            atoi(port.c_str()), user.c_str());
    } else if (param.find("host") != param.end()) {
        const std::string& host = param["host"];
        const std::string& port = param["port"];
        *fs = hdfsConnect(host.c_str(), atoi(port.c_str()));
        LOG(INFO, "hdfsConnect: %s:%d", host.c_str(), atoi(port.c_str()));
    } else {
        *fs = hdfsConnect("default", 0);
        LOG(INFO, "hdfsConnect: default user");
    }
}

void InfHdfs::Connect(const Param& param) {
    ConnectInfHdfs(param, &fs_);
}

bool InfHdfs::Open(const std::string& path, OpenMode mode) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    if (!fs_) {
        return false;
    }
    if (mode == kReadFile) {
        fd_ = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, 0, 0, 0);
    } else if (mode == kWriteFile) {
        short replica = 3;
        fd_ = hdfsOpenFile(fs_, path.c_str(), O_WRONLY|O_CREAT, 0, replica, 0);
    } else {
        LOG(WARNING, "unknown open mode.");
        return false;
    }
    if (!fd_) {
        LOG(WARNING, "open %s fail", path.c_str());
        return false;
    }
    return true;
}

bool InfHdfs::Close() {
    LOG(INFO, "try close file: %s", path_.c_str());
    if (!fs_) {
        return false;
    }
    if (!fd_) {
        return false;
    }
    int ret = hdfsCloseFile(fs_, fd_);
    if (ret != 0) {
        return false;
    }
    return true;
}

inline bool InfHdfs::Seek(int64_t pos) {
    return hdfsSeek(fs_, fd_, pos) == 0;
}

inline int32_t InfHdfs::Read(void* buf, size_t len) {
    return hdfsRead(fs_, fd_, buf, len);
}

inline int32_t InfHdfs::Write(void* buf, size_t len) {
    return hdfsWrite(fs_, fd_, buf, len);
}

inline int64_t InfHdfs::Tell() {
    return hdfsTell(fs_, fd_);
}

int64_t InfHdfs::GetSize() {
    hdfsFileInfo* info = NULL;
    info = hdfsGetPathInfo(fs_, path_.c_str());
    if (info == NULL) {
        LOG(WARNING, "failed to get info of %s", path_.c_str());
        return -1;
    }
    int64_t file_size = info->mSize;
    hdfsFreeFileInfo(info, 1);
    return file_size;
}

inline bool InfHdfs::Rename(const std::string& old_name, const std::string& new_name) {
    return hdfsRename(fs_, old_name.c_str(), new_name.c_str()) == 0;
}

inline bool InfHdfs::Remove(const std::string& path) {
    return hdfsDelete(fs_, path.c_str()) == 0;
}

bool InfHdfs::List(const std::string& dir, std::vector<FileInfo>* children) {
    if (children == NULL) {
        return false;
    }
    int file_num = 0;
    hdfsFileInfo* file_list = hdfsListDirectory(fs_, dir.c_str(), &file_num);
    if (file_list == NULL) {
        LOG(WARNING, "error in listing directory: %s", dir.c_str());
        return false;
    }
    for (int i = 0; i < file_num; i++) {
        children->push_back(FileInfo());
        FileInfo& last = children->back();
        last.kind = file_list[i].mKind;
        last.name = file_list[i].mName;
        last.size = file_list[i].mSize;
    }
    hdfsFreeFileInfo(file_list, file_num);
    return true;
}

bool InfHdfs::Glob(const std::string& dir, std::vector<FileInfo>* children) {
    if (children == NULL) {
        return false;
    }
    std::deque<std::string> prefixes;
    prefixes.push_back("");
    size_t start = 0;
    int file_num = 0;
    bool keep_loop = true;
    while (keep_loop) {
        size_t star = dir.find_first_of('*', start);
        size_t slash = dir.find_last_of('/', star);
        const std::string& cur = dir.substr(start, slash - start);
        start = dir.find_first_of('/', slash + 1);
        keep_loop = start != std::string::npos && start != dir.size() - 1;
        const std::string& pattern = dir.substr(slash + 1, start - slash - 1);
        size_t size = prefixes.size();
        for (size_t i = 0; i < size; ++i) {
            std::string pre = prefixes.front();
            prefixes.pop_front();
            std::string prefix = pre + cur;
            hdfsFileInfo* file_list = hdfsListDirectory(fs_, prefix.c_str(), &file_num);
            if (file_list == NULL) {
                continue;
            }
            for (int j = 0; j < file_num; ++j) {
                std::string cur_file;
                ParseHdfsAddress(file_list[j].mName, NULL, NULL, &cur_file);
                if (!PatternMatch(cur_file, prefix + "/" + pattern)) {
                    continue;
                }
                if (!keep_loop) {
                    prefixes.push_back(prefix);
                    break;
                } else {
                    prefixes.push_back(cur_file);
                }
            }
            hdfsFreeFileInfo(file_list, file_num);
        }
    }
    if (!prefixes.empty()) {
        for (std::deque<std::string>::iterator it = prefixes.begin();
                it != prefixes.end(); ++it) {
            hdfsFileInfo* file_list = hdfsListDirectory(fs_, it->c_str(), &file_num);
            if (file_list == NULL) {
                continue;
            }
            for (int i = 0; i < file_num; ++i) {
                std::string cur_file;
                ParseHdfsAddress(file_list[i].mName, NULL, NULL, &cur_file);
                if (PatternMatch(cur_file, dir)) {
                    children->push_back(FileInfo());
                    FileInfo& last = children->back();
                    last.kind = file_list[i].mKind;
                    last.name = file_list[i].mName;
                    last.size = file_list[i].mSize;
                }
            }
            hdfsFreeFileInfo(file_list, file_num);
        }
    }
    return true;
}

inline bool InfHdfs::Mkdirs(const std::string& dir) {
    return hdfsCreateDirectory(fs_, dir.c_str()) == 0;
}

inline bool InfHdfs::Exist(const std::string& path) {
    return hdfsExists(fs_, path.c_str()) == 0;
}

LocalFs::LocalFs() : fd_(0) {

}

bool LocalFs::Open(const std::string& path, OpenMode mode) {
    path_ = path;
    mode_t acl = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH; 
    if (mode == kReadFile) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            LOG(WARNING, "open %s fail, %s", path.c_str(), strerror(errno));
            return false;
        }
    } else if (mode == kWriteFile) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, acl);
        if (fd_ < 0) {
            LOG(WARNING, "open %s fail, %s", path.c_str(), strerror(errno));
            return false;
        }
    } else {
        LOG(WARNING, "unkown open mode");
        return false;
    }
    return true;
}

inline bool LocalFs::Close() {
    return ::close(fd_) == 0;
}

inline bool LocalFs::Seek(int64_t pos) {
    return ::lseek(fd_, pos, SEEK_SET) >= 0; 
}

inline int32_t LocalFs::Read(void* buf, size_t len) {
    return ::read(fd_, buf, len);
}

inline int32_t LocalFs::Write(void* buf, size_t len) {
    return ::write(fd_, buf, len);
}

inline int64_t LocalFs::Tell() {
    return ::lseek(fd_, 0, SEEK_CUR);
}

inline int64_t LocalFs::GetSize() {
    struct stat buf;
    fstat(fd_, &buf);
    int64_t size = buf.st_size;
    return size;
}

inline bool LocalFs::Rename(const std::string& old_name, const std::string& new_name) {
    return ::rename(old_name.c_str(), new_name.c_str()) == 0;
}

inline bool LocalFs::Remove(const std::string& path) {
    return ::remove(path.c_str()) == 0;
}

inline bool LocalFs::Mkdirs(const std::string& dir) {
    return ::mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0;
}

inline bool LocalFs::Exist(const std::string& path) {
    struct stat buffer;
    return ::stat(name.c_str(), &buffer) == 0;
}

inline FileHub* FileHub::GetHub() {
    return new FileHubImpl();
}

File::Param FileHub::BuildFileParam(DfsInfo& info) {
    File::Param param;
    if(!info.user().empty() && !info.password().empty()) {
        param["user"] = info.user();
        param["password"] = info.password();
    }
    if (boost::starts_with(info.path(), "hdfs://")) {
        std::string host;
        int port;
        std::string path;
        ParseHdfsAddress(info.path(), &host, &port, &path);
        param["host"] = host;
        param["port"] = boost::lexical_cast<std::string>(port);
        info.set_path(path);
        info.set_host(host);
        info.set_port(boost::lexical_cast<std::string>(port));
    } else if (!info.host().empty() && !info.port().empty()) {
        param["host"] = info.host();
        param["port"] = info.port();
    }
    return param;

}

File* FileHubImpl::BuildFs(DfsInfo& info) {
    File::Param param = BuildFileParam(info);
    const std::string& host = info.host();
    if (host.empty() || info.port().empty()) {
        return NULL;
    }
    std::string key = host + ":" + info.port();

    MutexLock lock(&mu_);
    if (fs_map_.find(key) == fs_map_.end()) {
        LOG(DEBUG, "get fs, host: %s, param.size(): %d", host.c_str(), param.size());
        File* fs = File::Create(kInfHdfs, param);
        fs_map_[key].reset(fs);
        param_map_[key] = param;
        return fs;
    }
    return fs_map_[key].get();
}

File* FileHubImpl::GetFs(const std::string& address) {
    std::string host;
    int port;
    ParseHdfsAddress(address, &host, &port, NULL);
    std::string key = host + ":" + boost::lexical_cast<std::string>(port);

    MutexLock lock(&mu_);
    if (fs_map_.find(key) == fs_map_.end()) {
        return NULL;
    }
    return fs_map_[key].get();
}

File::Param FileHubImpl::GetParam(const std::string& address) {
    std::string host;
    int port;
    ParseHdfsAddress(address, &host, &port, NULL);
    std::string key = host + ":" + boost::lexical_cast<std::string>(port);

    MutexLock lock(&mu_);
    if (param_map_.find(key) == param_map_.end()) {
        return File::Param();
    }
    return param_map_[key];
}

} //namespace shuttle
} //namespace baidu

