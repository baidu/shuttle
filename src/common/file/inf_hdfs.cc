#include "inf_hdfs.h"

#include <deque>
#include "logging.h"

namespace baidu {
namespace shuttle {

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
    if (file_num != 0 && file_list == NULL) {
        LOG(WARNING, "error in listing directory: %s", dir.c_str());
        return false;
    }
    if (file_num == 0) {
        LOG(DEBUG, "listing an empty dir: %s", dir.c_str());
        return true;
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

bool InfHdfs::Mkdir(const std::string& dir) {
    return hdfsCreateDirectory(fs_, dir.c_str()) == 0;
}

bool InfHdfs::Exist(const std::string& path) {
    return hdfsExists(fs_, path.c_str()) == 0;
}

}
}

