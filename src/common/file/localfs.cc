#include "localfs.h"

#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include "logging.h"

namespace baidu {
namespace shuttle {

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

bool LocalFs::List(const std::string& dir, std::vector<FileInfo>* children) {
    if (children == NULL) {
        return false;
    }
    DIR* dp = ::opendir(dir.c_str());
    if (dp == NULL) {
        LOG(WARNING, "error in opendir: %s", dir.c_str());
        return false;
    }
    const std::string& prefix = *dir.rbegin() == '/' ? dir : dir + '/';
    const std::string cur_dir(".");
    const std::string upward_dir("..");
    std::vector<std::string> files;
    struct dirent* ent;
    while ((ent = ::readdir(dp)) != NULL) {
        if (cur_dir == ent->d_name || upward_dir == ent->d_name) {
            continue;
        }
        files.push_back(prefix + ent->d_name);
    }
    ::closedir(dp);
    for (std::vector<std::string>::iterator it = files.begin();
            it != files.end(); ++it) {
        FileInfo info;
        struct stat st_buf;
        if (stat(it->c_str(), &st_buf) != 0) {
            continue;
        }
        info.name = *it;
        info.size = st_buf.st_size;
        if (S_ISREG(st_buf.st_mode)) {
            info.kind = 'F';
        } else if (S_ISDIR(st_buf.st_mode)) {
            info.kind = 'D';
        } else {
            info.kind = 'F';
        }
        children->push_back(info);
    }
    return true;
}

bool LocalFs::Mkdir(const std::string& dir) {
    return ::mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0;
}

bool LocalFs::Exist(const std::string& path) {
    struct stat buffer;
    return ::stat(path.c_str(), &buffer) == 0;
}


}
}

