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

namespace baidu {
namespace shuttle {

class InfHdfs : public File {
public:
    InfHdfs() : fs_(NULL), fd_(NULL) { }
    virtual ~InfHdfs() {
        Close();
        if (!fs_) {
            hdfsDisconnect(fs_);
        }
    }

    bool Connect(const Param& param);
    virtual bool Open(const std::string& path, OpenMode mode, const Param& param);
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
    virtual bool Mkdir(const std::string& dir);
    virtual bool Exist(const std::string& path);
    virtual std::string GetFileName() {
        return path_;
    }

private:
    hdfsFS fs_;
    hdfsFile fd_;
    std::string path_;
};

class LocalFs : public File {
public:
    LocalFs();
    virtual ~LocalFs() { }

    virtual bool Open(const std::string& path, OpenMode mode, const Param& param);
    virtual bool Close();
    virtual bool Seek(int64_t pos);
    virtual int32_t Read(void* buf, size_t len);
    virtual int32_t Write(void* buf, size_t len);
    virtual int64_t Tell();
    virtual int64_t GetSize();
    virtual bool Rename(const std::string& old_name, const std::string& new_name);
    virtual bool Remove(const std::string& path);
    virtual bool List(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        // TODO, not implement, not important for online functions
        return false;
    }
    virtual bool Glob(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        // TODO, not implement, not important for online functions
        return false;
    }
    virtual bool Mkdir(const std::string& dir);
    virtual bool Exist(const std::string& path);
    virtual std::string GetFileName() {
        return path_;
    }
private:
    int fd_;
    std::string path_;
};

template <class FileType>
class FileHubImpl : public FileHub<FileType> {
public:
    FileHubImpl() { }
    virtual ~FileHubImpl() { }

    virtual FileType* Store(const File::Param& param, FileType* fp);
    virtual FileType* Get(const std::string& address);
    virtual FileType* Get(const std::string& host, const std::string& port);
    virtual File::Param GetParam(const std::string& address);
    virtual File::Param GetParam(const std::string& host, const std::string& port);

private:
    std::map< std::string, boost::shared_ptr<FileType> > fmap_;
    std::map< std::string, File::Param > param_map_;
    Mutex mu_;
};

File* File::Create(FileType type, const Param& param) {
    switch(type) {
    case kLocalFs:
        return new LocalFs();
    case kInfHdfs:
        InfHdfs* fs = new InfHdfs();
        if (fs->Connect(param)) {
            return fs;
        }
        delete fs;
    }
    return NULL;
}

size_t File::ReadAll(void* buf, size_t len) {
    if (buf == NULL) {
        return (size_t)-1;
    }
    size_t cnt = 0;
    char* str = (char*)buf;
    while (cnt < len) {
        // Read 40kBi block at most
        size_t size = (len - cnt < 40960) ? (len - cnt) : 40960;
        int ret = Read(str + cnt, size);
        if (ret < 0) {
            return (size_t)-1;
        }
        if (ret == 0) {
            return cnt;
        }
        cnt += ret;
    }
    return cnt;
}

bool File::WriteAll(const void* buf, size_t len) {
    if (buf == NULL) {
        return false;
    }
    size_t start = 0;
    char* str = (char*)buf;
    while (start < len) {
        int write_bytes = Write(str + start, len - start);
        if (write_bytes < 0){
            return false;
        }
        start += write_bytes;
    }
    return true;
}

File::Param File::BuildParam(DfsInfo& info) {
    Param param;
    if(!info.user().empty() && !info.password().empty()) {
        param["user"] = info.user();
        param["password"] = info.password();
    }
    std::string host, port, path;
    if (ParseFullAddress(info.path(), &host, &port, &path)) {
        info.set_host(host);
        info.set_port(port);
        info.set_path(path);
    }
    if (info.has_host()) {
        param["host"] = info.host();
    }
    if (info.has_port()) {
        param["port"] = info.port();
    }
    return param;
}

bool File::ParseFullAddress(const std::string& address,
        std::string* host, std::string* port, std::string* path) {
    size_t header_len = 0;
    if (boost::starts_with(address, "file://")) {
        header_len = 7; // strlen("file://") == 7
    } else if (boost::starts_with(address, "hdfs://")) {
        header_len = 7; // strlen("hdfs://") == 7
    } else {
        LOG(DEBUG, "Not a full formatted address: %s", address.c_str());
        return false;
    }

    size_t server_path_separator = address.find_first_of('/', header_len);
    const std::string& server = address.substr(header_len, server_path_separator - header_len);
    size_t last_colon = server.find_last_of(':');
    if (last_colon == std::string::npos) {
        if (host != NULL) {
            *host = server;
        }
        if (port != NULL) {
            *port = "";
        }
    } else {
        if (host != NULL) {
            *host = server.substr(0, last_colon);
        }
        if (port != NULL) {
            *port = server.substr(last_colon + 1);
        }
    }
    if (path != NULL) {
        *path = address.substr(server_path_separator);
    }
    return true;
}

bool File::ConnectInfHdfs(const Param& param, void** fs) {
    if (fs == NULL) {
        return false;
    }
    if (param.find("user") != param.end()) {
        const std::string& user = param.find("user")->second;
        const std::string& password = param.find("password")->second;
        const std::string& host = param.find("host")->second;
        const std::string& port = param.find("port")->second;
        *fs = hdfsConnectAsUser(host.c_str(), atoi(port.c_str()),
                                user.c_str(),
                                password.c_str());
        LOG(INFO, "hdfsConnectAsUser: %s:%d, %s", host.c_str(),
            atoi(port.c_str()), user.c_str());
    } else if (param.find("host") != param.end()) {
        const std::string& host = param.find("host")->second;
        const std::string& port = param.find("port")->second;
        *fs = hdfsConnect(host.c_str(), atoi(port.c_str()));
        LOG(INFO, "hdfsConnect: %s:%d", host.c_str(), atoi(port.c_str()));
    } else {
        *fs = hdfsConnect("default", 0);
        LOG(INFO, "hdfsConnect: default user");
    }
    return *fs != NULL;
}

bool File::PatternMatch(const std::string& origin, const std::string& pattern) {
    const char* str = origin.c_str();
    const char* pat = pattern.c_str();
    const char* cp = NULL;
    const char* mp = NULL;

    while (*str && *pat != '*') {
        if (*pat != *str && *pat != '?') {
            return false;
        }
        ++str;
        ++pat;
    }

    while (*str) {
        if (*pat == '*') {
            if (!*++pat) {
                return true;
            }
            mp = pat;
            cp = str + 1;
        } else if (*pat == *str || *pat == '?') {
            ++pat;
            ++str;
        } else {
            pat = mp;
            str = cp++;
        }
    }

    while (*pat == '*') {
        ++pat;
    }
    return !*pat;
}

bool InfHdfs::Connect(const Param& param) {
    return ConnectInfHdfs(param, &fs_);
}

bool InfHdfs::Open(const std::string& path, OpenMode mode, const Param& param) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    if (!fs_) {
        return false;
    }
    if (mode == kReadFile) {
        Param::const_iterator decompress_iter = param.find("decompress");
        if (decompress_iter != param.end() && decompress_iter->second == "true") {
            CompressType type = gzip;
            Param::const_iterator format_iter = param.find("decompress_format");
            if (format_iter != param.end()) {
                const std::string& fmt = format_iter->second;
                if (fmt == "gzip") {
                    type = gzip;
                } else if (fmt == "bz") {
                    type = bzip;
                } else if (fmt == "lzma") {
                    type = lzma;
                } else if (fmt == "lzo") {
                    type = lzo;
                } else if (fmt == "qz") {
                    type = quicklz;
                } else {
                    LOG(WARNING, "unknown format: %s", fmt.c_str());
                }
            }
            fd_ = hdfsOpenFileWithDeCompress(fs_, path.c_str(), O_RDONLY, 0, 0, 0, type);
        } else {
            fd_ = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, 0, 0, 0);
        }
    } else if (mode == kWriteFile) {
        short replica = 3;
        Param::const_iterator replica_iter = param.find("replica");
        if (replica_iter != param.end()) {
            replica = atoi(replica_iter->second.c_str());
        }
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
    if (!fs_) {
        return false;
    }
    if (!fd_) {
        return false;
    }
    LOG(INFO, "try close file: %s", path_.c_str());
    int ret = hdfsCloseFile(fs_, fd_);
    if (ret != 0) {
        return false;
    }
    return true;
}

bool InfHdfs::Seek(int64_t pos) {
    return hdfsSeek(fs_, fd_, pos) == 0;
}

int32_t InfHdfs::Read(void* buf, size_t len) {
    return hdfsRead(fs_, fd_, buf, len);
}

int32_t InfHdfs::Write(void* buf, size_t len) {
    return hdfsWrite(fs_, fd_, buf, len);
}

int64_t InfHdfs::Tell() {
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

bool InfHdfs::Rename(const std::string& old_name, const std::string& new_name) {
    return hdfsRename(fs_, old_name.c_str(), new_name.c_str()) == 0;
}

bool InfHdfs::Remove(const std::string& path) {
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
                ParseFullAddress(file_list[j].mName, NULL, NULL, &cur_file);
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
                ParseFullAddress(file_list[i].mName, NULL, NULL, &cur_file);
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

bool InfHdfs::Mkdir(const std::string& dir) {
    return hdfsCreateDirectory(fs_, dir.c_str()) == 0;
}

bool InfHdfs::Exist(const std::string& path) {
    return hdfsExists(fs_, path.c_str()) == 0;
}

LocalFs::LocalFs() : fd_(0) {

}

bool LocalFs::Open(const std::string& path, OpenMode mode, const Param& /*param*/) {
    LOG(INFO, "try to open: %s", path.c_str());
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

bool LocalFs::Close() {
    LOG(INFO, "try close file: %s", path_.c_str());
    return ::close(fd_) == 0;
}

bool LocalFs::Seek(int64_t pos) {
    return ::lseek(fd_, pos, SEEK_SET) >= 0; 
}

int32_t LocalFs::Read(void* buf, size_t len) {
    return ::read(fd_, buf, len);
}

int32_t LocalFs::Write(void* buf, size_t len) {
    return ::write(fd_, buf, len);
}

int64_t LocalFs::Tell() {
    return ::lseek(fd_, 0, SEEK_CUR);
}

int64_t LocalFs::GetSize() {
    struct stat buf;
    fstat(fd_, &buf);
    int64_t size = buf.st_size;
    return size;
}

bool LocalFs::Rename(const std::string& old_name, const std::string& new_name) {
    return ::rename(old_name.c_str(), new_name.c_str()) == 0;
}

bool LocalFs::Remove(const std::string& path) {
    return ::remove(path.c_str()) == 0;
}

bool LocalFs::Mkdir(const std::string& dir) {
    return ::mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0;
}

bool LocalFs::Exist(const std::string& path) {
    struct stat buffer;
    return ::stat(path.c_str(), &buffer) == 0;
}

template <class FileType>
FileHub<FileType>* FileHub<FileType>::GetHub() {
    return new FileHubImpl<FileType>();
}

template <class FileType>
FileType* FileHubImpl<FileType>::Store(const File::Param& param, FileType* fp) {
    if (param.find("host") == param.end() || param.find("port") == param.end()) {
        return NULL;
    }
    const std::string& host = param.find("host")->second;
    const std::string& port = param.find("port")->second;
    std::string key = host + ":" + port;

    MutexLock lock(&mu_);
    if (fmap_.find(key) == fmap_.end()) {
        LOG(DEBUG, "get fs, host: %s, param.size(): %d", host.c_str(), param.size());
        fmap_[key].reset(fp);
        param_map_[key] = param;
        return fp;
    }
    return fmap_[key].get();
}

template <class FileType>
FileType* FileHubImpl<FileType>::Get(const std::string& address) {
    std::string host, port;
    File::ParseFullAddress(address, &host, &port, NULL);
    return Get(host, port);
}

template <class FileType>
FileType* FileHubImpl<FileType>::Get(const std::string& host, const std::string& port) {
    std::string key = host + ":" + port;

    MutexLock lock(&mu_);
    if (fmap_.find(key) == fmap_.end()) {
        return NULL;
    }
    return fmap_[key].get();
}

template <class FileType>
File::Param FileHubImpl<FileType>::GetParam(const std::string& address) {
    std::string host, port;
    File::ParseFullAddress(address, &host, &port, NULL);
    return GetParam(host, port);
}

template <class FileType>
File::Param FileHubImpl<FileType>::GetParam(const std::string& host, const std::string& port) {
    std::string key = host + ":" + port;

    MutexLock lock(&mu_);
    if (param_map_.find(key) == param_map_.end()) {
        return File::Param();
    }
    return param_map_[key];
}

}
}

