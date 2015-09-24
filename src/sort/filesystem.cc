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
private:
    hdfsFS fs_;
    hdfsFile fd_;
    std::string path_;
};

FileSystem* FileSystem::CreateInfHdfs() {
    return new InfHdfs();
}

bool InfHdfs::Open(const std::string& path, Param param, OpenMode mode) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    if (param.size() == 0) {
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
        fd_ = hdfsOpenFile(fs_, path.c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
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

} //namespace shuttle
} //namespace baidu
