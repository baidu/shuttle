#include <fcntl.h> 
#include <stdio.h> 
#include <sys/stat.h> 
#include <sys/types.h> 
#include <unistd.h> 
#include "filesystem.h"
#include "hdfs.h" //for hdfs of inf
#include "logging.h"

using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

class InfHdfs : public FileSystem {
public:
    bool Open(const std::string& path, 
              Param param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    virtual ~InfHdfs(){};
private:
    hdfsFS fs_;
    hdfsFile fd_;
    std::string path_;
};


class LocalFs : public FileSystem {
public:
    bool Open(const std::string& path, 
              Param param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    virtual ~LocalFs(){};
private:
    int fd_;
    std::string path_;
};

FileSystem* FileSystem::CreateInfHdfs() {
    return new InfHdfs();
}

FileSystem* FileSystem::CreateLocalFs() {
    return new LocalFs();
}

bool InfHdfs::Open(const std::string& path, Param param, OpenMode mode) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    if (param.find("user") == param.end()) {
        fs_ = hdfsConnect("default", 0);
    } else {
        const std::string& user = param["user"];
        const std::string& password = param["passowrd"];
        const std::string& host = param["host"];
        const std::string& port = param["port"];
        fs_ = hdfsConnectAsUser(host.c_str(), atoi(port.c_str()),
                                user.c_str(), password.c_str());
    }
    if (!fs_) {
        return false;
    }
    if (mode == kReadFile) {
        fd_ = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, 0, 0, 0);
    } else if (mode == kWriteFile) {
        short replica = 3;
        if (param.find("replica") != param.end()) {
            replica = atoi(param["replica"].c_str());
        }
        //printf("replica: %d, %s\n", replica, path.c_str());
        fd_ = hdfsOpenFile(fs_, path.c_str(), O_WRONLY|O_CREAT, 0, replica, 0);
    } else {
        LOG(WARNING, "unkonw open mode.");
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

bool LocalFs::Open(const std::string& path, 
                   Param param,
                   OpenMode mode) {
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
    return lseek(fd_, 0, SEEK_CUR);
}

int64_t LocalFs::GetSize() {
    struct stat buf;
    fstat(fd_, &buf);
    int64_t size = buf.st_size;
    return size;
}

} //namespace shuttle
} //namespace baidu
