#include "sort_file_hdfs.h"

namespace baidu {
namespace shuttle {

SortFileHdfsReader::IteratorHdfs::IteratorHdfs(const std::string& start_key,
                                               const std::string& end_key,
                                               SortFileHdfsReader* reader) {
    reader_ = reader;
}

SortFileHdfsReader::IteratorHdfs::~IteratorHdfs() {

}

bool SortFileHdfsReader::IteratorHdfs::Done() {
    return false;
}

void SortFileHdfsReader::IteratorHdfs::Next() {

}

const std::string& SortFileHdfsReader::IteratorHdfs::Key() {
    return "";
}

const std::string& SortFileHdfsReader::IteratorHdfs::Value() {
    return "";
}

Status SortFileHdfsReader::IteratorHdfs::Error() {
    return kOk;
}

Status SortFileHdfsReader::Open(const std::string& path, Param& param) {
    return kOk;
}

SortFileReader::Iterator* SortFileHdfsReader::Scan(const std::string& start_key, 
                                                   const std::string& end_key) {
    return NULL;
}

Status SortFileHdfsReader::Close() {
    if (!fs_) {
        return kConnHdfsFail;
    }
    if (!fd_) {
        return kOpenHdfsFileFail;
    }
    int ret = hdfsCloseFile(fs_, fd_);
    if (ret != 0) {
        return kCloseFileFail;
    }
    return kOk;
}

Status SortFileHdfsWriter::Open(const std::string& path, Param& param) {
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
        return kConnHdfsFail;
    }
    fd_ = hdfsOpenFile(fs_, path.c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
    if (!fd_) {
        return kOpenHdfsFileFail;
    }
    return kOk;
}

Status SortFileHdfsWriter::Put(const std::string& key, const std::string& value) {
    return kOk;
}

Status SortFileHdfsWriter::Close() {
    return kOk;
}

}
}
