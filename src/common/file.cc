#include "file.h"

#include "file/localfs.h"
#include "file/inf_hdfs.h"

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <cstdio>
#include "logging.h"
#include "mutex.h"

// For templatize FileHub
#include "common/fileformat.h"
#include "common/scanner.h"

namespace baidu {
namespace shuttle {

// For simple unittest testcases
template class FileHub<int>;
// For potential usage
template class FileHub<File>;
template class FileHub<FormattedFile>;
template class FileHub<Scanner>;

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

template <class FileType>
FileHub<FileType>* FileHub<FileType>::GetHub() {
    return new FileHubImpl<FileType>();
}

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

File* File::Get(FileType type, void* ptr) {
    switch(type) {
    case kLocalFs:
        int fno = ::fileno(static_cast<FILE*>(ptr));
        if (fno != -1) {
            return new LocalFs(fno);
        }
    case kInfHdfs:
        if (ptr != NULL) {
            return new InfHdfs(static_cast<hdfsFS>(ptr));
        }
    }
    return NULL;
}

size_t File::ReadAll(void* buf, size_t len) {
    if (buf == NULL) {
        return static_cast<size_t>(-1);
    }
    size_t cnt = 0;
    char* str = (char*)buf;
    while (cnt < len) {
        // Read 40kBi block at most
        size_t size = (len - cnt < 40960) ? (len - cnt) : 40960;
        int ret = Read(str + cnt, size);
        if (ret < 0) {
            return static_cast<size_t>(-1);
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

File::Param File::BuildParam(const DfsInfo& info) {
    Param param;
    if(!info.user().empty() && !info.password().empty()) {
        param["user"] = info.user();
        param["password"] = info.password();
    }
    std::string host, port, path;
    if (ParseFullAddress(info.path(), NULL, &host, &port, &path)) {
        param["host"] = host;
        param["port"] = port;
    } else {
        if (info.has_host()) {
            param["host"] = info.host();
        }
        if (info.has_port()) {
            param["port"] = info.port();
        }
    }
    return param;
}

bool File::ParseFullAddress(const std::string& address,
        FileType* type, std::string* host, std::string* port, std::string* path) {
    size_t header_len = 0;
    FileType filetype = kLocalFs;
    if (boost::starts_with(address, "file://")) {
        header_len = 7; // strlen("file://") == 7
        filetype = kLocalFs;
    } else if (boost::starts_with(address, "hdfs://")) {
        header_len = 7; // strlen("hdfs://") == 7
        filetype = kInfHdfs;
    } else {
        LOG(DEBUG, "Not a full formatted address: %s", address.c_str());
        return false;
    }
    if (type != NULL) {
        *type = filetype;
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
    File::ParseFullAddress(address, NULL, &host, &port, NULL);
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
    File::ParseFullAddress(address, NULL, &host, &port, NULL);
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

