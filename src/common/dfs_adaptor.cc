#include "dfs_adaptor.h"

#include <boost/algorithm/string.hpp>

#include "logging.h"

namespace baidu {
namespace shuttle {

DfsAdaptor::DfsAdaptor(const std::string& dfs_url, const char* options) {
    ParseHdfsPath(dfs_url);

    fs_ = NULL;
    fs_ = hdfsConnect(dfs_server_.c_str(), 0);
    if (fs_ == NULL) {
        LOG(WARNING, "cannot connect to hdfs server: %s", dfs_server_.c_str());
        return;
    }

    current_file_ = NULL;
    if (!Open(dfs_path_.c_str(), options)) {
        LOG(WARNING, "cannot open hdfs file: %s", dfs_path_.c_str());
    }
}

DfsAdaptor::DfsAdaptor(const std::string& dfs_server) : dfs_server_(dfs_server) {
    fs_ = hdfsConnect(dfs_server.c_str(), 0);
    if (fs_ == NULL) {
        LOG(WARNING, "cannot connect to hdfs server: %s", dfs_server_.c_str());
    }
}

DfsAdaptor::~DfsAdaptor() {
    Close();
    if (fs_ != NULL) {
        hdfsDisconnect(fs_);
    }
}

bool DfsAdaptor::Open(const char* path, const char* options) {
    Close();
    current_options_ = 0;
    if (*options == 'r') {
        current_options_ = O_RDONLY;
    } else if (*options == 'w') {
        current_options_ = O_WRONLY | O_CREAT;
    } else if (*options == 'a') {
        current_options_ = O_WRONLY | O_APPEND | O_CREAT;
    }
    if (*(options + 1) == '+') {
        current_options_ = O_RDWR | O_CREAT;
        if (*options == 'a') {
            current_options_ = O_RDWR | O_APPEND | O_CREAT;
        }
    }
    current_file_ = hdfsOpenFile(fs_, path, current_options_, 0, 0, 0);
    dfs_path_ = path;
    return current_file_ != NULL;
}

void DfsAdaptor::Close() {
    if (current_file_ != NULL) {
        hdfsCloseFile(fs_, current_file_);
        current_file_ = NULL;
    }
}

int DfsAdaptor::Read(void* buffer, size_t size) {
    if (!(current_options_ & O_RDONLY)) {
        return 0;
    }
    return hdfsRead(fs_, current_file_, buffer, size);
}

int DfsAdaptor::Write(void* buffer, size_t size) {
    if (!(current_options_ & O_WRONLY)) {
        return 0;
    }
    return hdfsWrite(fs_, current_file_, buffer, size);
}

void DfsAdaptor::Flush() {
    if (!(current_options_ & O_WRONLY)) {
        return;
    }
    hdfsFlush(fs_, current_file_);
}

bool DfsAdaptor::ListDirectory(const std::string& dir, std::vector<FileInfo>& files) {
    if (!hdfsExists(fs_, dir.c_str())) {
        LOG(WARNING, "directory not exist: %s", dir.c_str());
        return false;
    }

    int file_num = 0;
    hdfsFileInfo* file_list = hdfsListDirectory(fs_, dir.c_str(), &file_num);
    if (file_list == NULL) {
        LOG(WARNING, "unexpected error in listing directory: %s", dir.c_str());
        return false;
    }
    for (int i = 0; i < file_num; i++) {
        files.push_back(FileInfo(file_list[i]));
    }
    hdfsFreeFileInfo(file_list, file_num);
    return true;
}

std::string DfsAdaptor::GetServerFromPath(const std::string& path) {
    // A hdfs path is composed with:
    // * A magic number: `hdfs://`
    // * A server hostname/ip:port format: `localhost:9090`
    // * An absolute path: `/somepath/somefile`
    // string must not contain trailing spaces and enters
    if (!boost::starts_with(path, "hdfs://")) {
        LOG(WARNING, "not a valid hdfs path: %s", path.c_str());
    }
    // len('hdfs://') + 1 == 7
    size_t server_path_seperator = path.find_first_of('/', 7);
    return path.substr(7, server_path_seperator - 7);
}

void DfsAdaptor::ParseHdfsPath(const std::string& path) {
    if (!boost::starts_with(path, "hdfs://")) {
        LOG(WARNING, "not a valid hdfs path: %s", path.c_str());
    }
    // len('hdfs://') + 1 == 7
    size_t server_path_seperator = path.find_first_of('/', 7);
    dfs_server_ = path.substr(7, server_path_seperator - 7);
    dfs_path_ = path.substr(server_path_seperator);
}

}
}

