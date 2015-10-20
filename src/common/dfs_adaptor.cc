#include "dfs_adaptor.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <string>

#include "logging.h"

namespace baidu {
namespace shuttle {

size_t DfsAdaptor::glob_bound = 100000;

DfsAdaptor::DfsAdaptor(const std::string& dfs_url, const char* options) {
    ParseHdfsPath(dfs_url);

    fs_ = NULL;
    Connect(dfs_server_);
    if (fs_ == NULL) {
        return;
    }

    current_file_ = NULL;
    if (!Open(dfs_path_.c_str(), options)) {
        LOG(WARNING, "cannot open hdfs file: %s", dfs_path_.c_str());
    }
}

DfsAdaptor::DfsAdaptor(const std::string& dfs_server) : dfs_server_(dfs_server) {
    Connect(dfs_server);
}

DfsAdaptor::~DfsAdaptor() {
    Close();
    if (fs_ != NULL) {
        hdfsDisconnect(fs_);
    }
}

bool DfsAdaptor::Connect(const std::string& host, int port) {
    if (fs_ != NULL) {
        LOG(INFO, "dfs adaptor has connected");
        return false;
    }
    fs_ = hdfsConnect(host.c_str(), port);
    if (fs_ == NULL) {
        LOG(WARNING, "cannot connect to hdfs server: %s:%d", host.c_str(), port);
        return false;
    }
    dfs_server_ = host + ":" + boost::lexical_cast<std::string>(port);
    return true;
}

bool DfsAdaptor::Connect(const std::string& server) {
    int last_colon = server.find_last_of(':');
    std::string server_host = server.substr(0, last_colon);
    int port = boost::lexical_cast<int>(server.substr(last_colon + 1));
    return Connect(server_host, port);
}

bool DfsAdaptor::Disconnect() {
    if (fs_ == NULL) {
        LOG(INFO, "dfs adaptor has disconnected");
        return true;
    }
    if (hdfsDisconnect(fs_)) {
        LOG(WARNING, "cannot disconnect to hdfs server: %s", dfs_server_.c_str());
        return false;
    }
    dfs_server_ = "";
    return true;
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
    ParseHdfsPath(dir);
    if (hdfsExists(fs_, dfs_path_.c_str())) {
        LOG(WARNING, "directory not exist: %s", dfs_path_.c_str());
        return false;
    }

    int file_num = 0;
    hdfsFileInfo* file_list = hdfsListDirectory(fs_, dfs_path_.c_str(), &file_num);
    if (file_list == NULL) {
        LOG(WARNING, "unexpected error in listing directory: %s", dfs_path_.c_str());
        return false;
    }
    for (int i = 0; i < file_num; i++) {
        files.push_back(FileInfo(file_list[i]));
    }
    hdfsFreeFileInfo(file_list, file_num);
    return true;
}

static bool pattern_match(const std::string& origin, const std::string& pattern) {
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

bool DfsAdaptor::GlobDirectory(const std::string& dir, std::vector<FileInfo>& files) {
    ParseHdfsPath(dir);

    std::deque<std::string> prefixes;
    std::string header = "hdfs://";
    header += dfs_server_;
    prefixes.push_back(header);
    size_t start = 0;
    int file_num = 0;
    bool keep_loop = true;
    while (keep_loop) {
        size_t star = dfs_path_.find_first_of('*', start);
        size_t slash = dfs_path_.find_last_of('/', star);
        const std::string& cur = dfs_path_.substr(start, slash - start);
        start = dfs_path_.find_first_of('/', slash + 1);
        keep_loop = start != std::string::npos && start != dfs_path_.size() - 1;
        const std::string& pattern = dfs_path_.substr(slash + 1, start - slash - 1);
        size_t size = prefixes.size();
        for (size_t i = 0; i < size; ++i) {
            const std::string& pre = prefixes.front();
            prefixes.pop_back();
            std::string prefix = pre + cur;
            hdfsFileInfo* file_list = hdfsListDirectory(fs_, prefix.c_str(), &file_num);
            if (file_list == NULL) {
                continue;
            }
            for (int j = 0; j < file_num; ++j) {
                std::string cur_file = file_list[j].mName;
                if (!pattern_match(cur_file, prefix + "/" + pattern)) {
                    continue;
                }
                if (!keep_loop) {
                    prefixes.push_back(prefix);
                    break;
                } else {
                    prefixes.push_back(prefix + cur_file);
                }
            }
            hdfsFreeFileInfo(file_list, file_num);
        }
        if (!keep_loop && prefixes.size() > glob_bound) {
            LOG(WARNING, "too much files to list");
            return false;
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
                if (pattern_match(file_list[i].mName, dir)) {
                    files.push_back(FileInfo(file_list[i]));
                }
            }
            hdfsFreeFileInfo(file_list, file_num);
        }
    }
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

