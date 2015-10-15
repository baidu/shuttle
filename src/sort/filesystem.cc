#include <fcntl.h> 
#include <stdio.h> 
#include <sys/stat.h> 
#include <sys/types.h> 
#include <unistd.h> 
#include "filesystem.h"
#include "logging.h"

using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

class InfHdfs : public FileSystem {
public:
    InfHdfs();
    static void ConnectInfHdfs(Param param, hdfsFS* fs);
    void Connect(Param param);
    bool Open(const std::string& path, 
              Param param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    bool Rename(const std::string& old_name, const std::string& new_name);
    virtual ~InfHdfs(){};
    bool List(const std::string& dir, std::vector<std::string>* children);
    bool Mkdirs(const std::string& dir);
    bool Exist(const std::string& path);
private:
    hdfsFS fs_;
    hdfsFile fd_;
    std::string path_;
};


class LocalFs : public FileSystem {
public:
    LocalFs();
    bool Open(const std::string& path, 
              Param param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    bool Rename(const std::string& old_name, const std::string& new_name);
    virtual ~LocalFs(){};
    bool List(const std::string& dir, std::vector<std::string>* children) {
        (void)dir;
        (void)children;
        return false; //TODO, not implementation
    }
    bool Mkdirs(const std::string& dir) {
        (void)dir;
        return false; //TODO, not implementation
    }
    bool Exist(const std::string& path) {
        (void)path;
        return false; //TODO, not implementation
    }
private:
    int fd_;
    std::string path_;
};

FileSystem* FileSystem::CreateInfHdfs() {
    return new InfHdfs();
}

FileSystem* FileSystem::CreateInfHdfs(Param param) {
    InfHdfs* fs = new InfHdfs();
    fs->Connect(param);
    return fs;
}

FileSystem* FileSystem::CreateLocalFs() {
    return new LocalFs();
}

bool FileSystem::WriteAll(void* buf, size_t len) {
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

void InfHdfs::ConnectInfHdfs(Param param, hdfsFS* fs) {
    if (param.find("user") == param.end()) {
        *fs = hdfsConnect("default", 0);
    } else {
        const std::string& user = param["user"];
        const std::string& password = param["password"];
        const std::string& host = param["host"];
        const std::string& port = param["port"];
        *fs = hdfsConnectAsUser(host.c_str(), atoi(port.c_str()),
                                user.c_str(),
                                password.c_str());
    }
}

void InfHdfs::Connect(Param param) {
    ConnectInfHdfs(param, &fs_);
}

bool InfHdfs::Open(const std::string& path, Param param, OpenMode mode) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    Connect(param);
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

bool InfHdfs::Rename(const std::string& old_name, const std::string& new_name) {
    return hdfsRename(fs_, old_name.c_str(), new_name.c_str()) == 0;
}

bool InfHdfs::List(const std::string& dir, std::vector<std::string>* children) {
    int file_num = 0;
    hdfsFileInfo* file_list = hdfsListDirectory(fs_, dir.c_str(), &file_num);
    if (file_list == NULL) {
        LOG(WARNING, "error in listing directory: %s", dir.c_str());
        return false;
    }
    for (int i = 0; i < file_num; i++) {
        children->push_back(file_list[i].mName);
    }
    hdfsFreeFileInfo(file_list, file_num);
    return true;
}

bool InfHdfs::Mkdirs(const std::string& dir) {
    return hdfsCreateDirectory(fs_, dir.c_str()) == 0;
}

bool InfHdfs::Exist(const std::string& path) {
    return hdfsExists(fs_, path.c_str()) == 0;
}

LocalFs::LocalFs() : fd_(0) {

}

bool LocalFs::Open(const std::string& path, 
                   Param param,
                   OpenMode mode) {
    (void)param;
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

bool LocalFs::Rename(const std::string& old_name, const std::string& new_name) {
    return ::rename(old_name.c_str(), new_name.c_str()) == 0;
}

InfSeqFile::InfSeqFile() : fs_(NULL), sf_(NULL) {

}

bool InfSeqFile::Open(const std::string& path, FileSystem::Param param, OpenMode mode) {
    InfHdfs::ConnectInfHdfs(param, &fs_);
    if (!fs_) {
        LOG(WARNING, "connect hdfs fail, when try open: %s", path.c_str());
        return false;
    }
    if (mode == kReadFile) {
        sf_ = readSequenceFile(fs_, path.c_str());
        if (!sf_) {
            LOG(WARNING, "fail to read: %s", path.c_str());
            return false;
        }
    } else if (mode == kWriteFile) {
        sf_ = writeSequenceFile(fs_, path.c_str(), "BLOCK", "org.apache.hadoop.io.compress.LzoCodec");
        if (!sf_) {
            LOG(WARNING, "fail to write: %s", path.c_str());
            return false;
        }
    } else {
        LOG(FATAL, "unkown mode: %d", mode);
    }
    path_ = path;
    return true;
}

bool InfSeqFile::Close() {
    return closeSequenceFile(fs_, sf_) == 0;
}

bool InfSeqFile::ReadNextRecord(std::string* key, std::string* value, bool* eof) {
    int key_len;
    int value_len;
    void* raw_key;
    void* raw_value;
    *eof = false;
    int ret = readNextRecordFromSeqFile(fs_, sf_, &raw_key, &key_len, &raw_value, &value_len);
    if (ret != 0 && ret != 1) {
        LOG(WARNING, "fail to read next record: %s", path_.c_str());
        return false;
    }
    if (ret == 1) {
        *eof = true;
        return true;
    }
    key->assign(static_cast<const char*>(raw_key), key_len);
    value->assign(static_cast<const char*>(raw_value), value_len);
    return true;
}

bool InfSeqFile::WriteNextRecord(const std::string& key, const std::string& value) {
    int ret = writeRecordIntoSeqFile(fs_, sf_, key.data(), key.size(), value.data(), value.size());
    if (ret != 0) {
        LOG(WARNING, "fail to write next record: %s", path_.c_str());
        return false;
    }
    return true;
}

bool InfSeqFile::Seek(int64_t offset) {
    int64_t ret = syncSeqFile(sf_, offset);
    if (ret < 0) {
        LOG(WARNING, "fail to seek: %s, %ld", path_.c_str(), offset);
        return false;
    }
    return true;
}

int64_t InfSeqFile::Tell() {
    return getSeqFilePos(sf_);
}

} //namespace shuttle
} //namespace baidu
