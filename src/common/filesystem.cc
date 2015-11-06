#include <deque>
#include <fcntl.h> 
#include <stdio.h> 
#include <sys/stat.h> 
#include <sys/types.h> 
#include <unistd.h> 

#include "filesystem.h"
#include "logging.h"
#include "common/tools_util.h"
#include "mutex.h"
#include "nfs.h"

using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

class InfHdfs : public FileSystem {
public:
    InfHdfs();
    virtual ~InfHdfs() { }
    static void ConnectInfHdfs(Param& param, hdfsFS* fs);
    void Connect(Param& param);
    bool Open(const std::string& path,
              OpenMode mode);
    bool Open(const std::string& path, 
              Param& param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    bool Rename(const std::string& old_name, const std::string& new_name);
    bool Remove(const std::string& path);
    bool List(const std::string& dir, std::vector<FileInfo>* children);
    bool Glob(const std::string& dir, std::vector<FileInfo>* children);
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
    virtual ~LocalFs() { }
    bool Open(const std::string& path,
              OpenMode mode);
    bool Open(const std::string& path, 
              Param& param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    bool Rename(const std::string& old_name, const std::string& new_name);
    bool Remove(const std::string& /*path*/) {
        //TODO, not implementation
        return false;
    }
    bool List(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        //TODO, not implementation
        return false;
    }
    bool Glob(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        //TODO, not implementation
        return false;
    }
    bool Mkdirs(const std::string& /*dir*/) {
        //TODO, not implementation
        return false;
    }
    bool Exist(const std::string& /*path*/) {
        //TODO, not implementation
        return false;
    }
private:
    int fd_;
    std::string path_;
};

class Nfs : public FileSystem {
public:
    Nfs();
    virtual ~Nfs();
    void Connect(Param& param);
    bool Open(const std::string& path,
              OpenMode mode);
    bool Open(const std::string& path,
              Param& param,
              OpenMode mode);
    bool Close();
    bool Seek(int64_t pos);
    int32_t Read(void* buf, size_t len);
    int32_t Write(void* buf, size_t len);
    int64_t Tell();
    int64_t GetSize();
    bool Rename(const std::string& old_name, const std::string& new_name);
    bool Remove(const std::string& path);
    bool List(const std::string& dir, std::vector<FileInfo>* children);
    bool Glob(const std::string&, std::vector<FileInfo>* ) {return false;}
    bool Mkdirs(const std::string& dir);
    bool Exist(const std::string& path);
private:
    nfs::NFS* fs_;
    nfs::NFSFILE* fd_;
    std::string path_;
};

FileSystem* FileSystem::CreateInfHdfs() {
    return new InfHdfs();
}

FileSystem* FileSystem::CreateInfHdfs(Param& param) {
    InfHdfs* fs = new InfHdfs();
    fs->Connect(param);
    return fs;
}

FileSystem* FileSystem::CreateNfs() {
    return new Nfs();
}

FileSystem* FileSystem::CreateNfs(Param& param) {
    Nfs* fs = new Nfs();
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

void InfHdfs::ConnectInfHdfs(Param& param, hdfsFS* fs) {
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

void InfHdfs::Connect(Param& param) {
    ConnectInfHdfs(param, &fs_);
}

bool InfHdfs::Open(const std::string& path, OpenMode mode) {
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

bool InfHdfs::Open(const std::string& path, Param& param, OpenMode mode) {
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
        children->push_back(FileInfo(file_list[i]));
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
                    children->push_back(FileInfo(file_list[i]));
                }
            }
            hdfsFreeFileInfo(file_list, file_num);
        }
    }
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

bool LocalFs::Open(const std::string& path, 
                   Param& /*param*/,
                   OpenMode mode) {
    return Open(path, mode);
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

bool InfSeqFile::Open(const std::string& path, FileSystem::Param& param, OpenMode mode) {
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


Nfs::Nfs() : fs_(NULL), fd_(NULL){
}

Nfs::~Nfs() {
    /*if (fs_) {
        fs_->Destroy();
        delete fs_;
    }*/
    if (fd_) {
        free(fd_);
    }
}

void Nfs::Connect(Param& param) {
    static Mutex s_mu;
    static nfs::NFS* s_fs = NULL;
    MutexLock lock(&s_mu);
    if (s_fs != NULL) {
        fs_ = s_fs;
        return;
    }
    fs_ = nfs::NFS::CreateNFS();
    nfs::SetComlogLevel(2);
    if (param.find("host") == param.end()) {
        fs_->Init(sNfsMountPoint.c_str(), sNfsConfName.c_str());
    } else {
        nfs::NfsOptions options;
        options.username = param["user"].c_str();
        options.password = param["password"].c_str();
        options.master_ip = param["host"].c_str();
        options.master_port = atoi(param["port"].c_str());
        fs_->Init(sNfsMountPoint.c_str(), options);
    }
    s_fs = fs_;
}

bool Nfs::Open(const std::string& path, OpenMode mode) {
    path_ = path;
    LOG(INFO, "try to open %s", path.c_str());
    if (mode == kReadFile) {
        fd_ = fs_->Open(path.c_str(), "r");
    } else if (mode == kWriteFile) {
        fd_ = fs_->Open(path.c_str(), "w");
    }
    return fd_ != NULL;
}

bool Nfs::Open(const std::string& path, Param& param, OpenMode mode) {
    if (fs_ == NULL) {
        Connect(param);
    }
    return Open(path, mode);
}

bool Nfs::Close() {
    if (!fd_) {
        return false;
    }
    LOG(INFO, "try to close: %s", path_.c_str());
    return nfs::Close(fd_) == 0;
}

bool Nfs::Seek(int64_t pos) {
    if (!fd_) {
        return false;
    }
    return nfs::Seek(fd_, pos) == 0;
}

int32_t Nfs::Read(void* buf, size_t len) {
    return nfs::Read(fd_, buf, len);
}

int32_t Nfs::Write(void* buf, size_t len) {
    return nfs::Write(fd_, buf, len);
}

int64_t Nfs::Tell() {
    return nfs::Tell(fd_);
}

int64_t Nfs::GetSize() {
    struct ::stat st;
    if (fs_->Stat(path_.c_str(), &st) == 0){
        return st.st_size;
    }
    return 0;
}

bool Nfs::Rename(const std::string& old_name,
                 const std::string& new_name) {
    if (!fs_) {
        return false;
    }
    return fs_->Rename(old_name.c_str(), new_name.c_str()) == 0;
}

bool Nfs::Remove(const std::string& path) {
    if (!fs_) {
        return false;
    }
    return fs_->Unlink(path.c_str()) == 0;
}

bool Nfs::List(const std::string& dir_name, std::vector<FileInfo>* children) {
    struct dirent *ptr = NULL;
    if (!fs_) {
        return false;
    }
    nfs::NFSDIR*  dir = fs_->Opendir(dir_name.c_str());
    if (dir == NULL) {
        nfs::Closedir(dir);
        return false;
    }
    while ((ptr = nfs::Readdir(dir)) != NULL) {
        if (strcmp(ptr->d_name, ".") != 0 && strcmp(ptr->d_name, "..") != 0) {
            FileInfo fileinfo;
            fileinfo.name = ptr->d_name;
            children->push_back(fileinfo);
        }
    }
    nfs::Closedir(dir);
    return true;
}

bool Nfs::Mkdirs(const std::string& dir_path) {
    if (!fs_) {
        return false;
    }
    size_t beg = 0;
    size_t seg = dir_path.find('/', beg);
    while (seg != std::string::npos) {
        if (seg + 1 >= dir_path.size()) {
            break;
        }
        fs_->Mkdir(dir_path.substr(0, seg + 1).c_str());
        beg = seg + 1;
        seg = dir_path.find('/', beg);
    }
    return fs_->Mkdir(dir_path.c_str()) == 0;
}

bool Nfs::Exist(const std::string& path) {
    if (!fs_) {
        return false;
    }
    return fs_->Access(path.c_str(), R_OK) == 0;
}

} //namespace shuttle
} //namespace baidu

