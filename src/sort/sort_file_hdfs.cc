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

Status SortFileHdfsReader::Open(const std::string& path, const Param& param) {
    return kOk;
}

SortFileReader::Iterator* SortFileHdfsReader::Scan(const std::string& start_key, 
                                                   const std::string& end_key) {
    return NULL;
}

Status SortFileHdfsReader::Close() {
    return kOk;
}

Status SortFileHdfsWriter::Open(const std::string& path, const Param& param) {
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
