#include "sort_file_hdfs.h"
#include <algorithm>
#include <stdint.h>
#include <snappy.h>
#include <logging.h>

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

const static int32_t sBlockSize = (32 << 10);
const static int32_t sMagicNumber = 25997;

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
    return kOk;
}

SortFileHdfsWriter::SortFileHdfsWriter() : fs_(NULL),
                                           fd_(NULL),
                                           cur_block_size_(0) {

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
        LOG(WARNING, "open %s for write fail", path.c_str());
        return kOpenHdfsFileFail;
    }
    return kOk;
}

Status SortFileHdfsWriter::Put(const std::string& key, const std::string& value) {
    if (key < last_key_) {
        LOG(WARNING, "try to put a un-ordered key: %s", key.c_str());
        return kInvalidArg;
    }
    if (cur_block_size_ >= sBlockSize) {
        Status status = FlushCurBlock();
        if (status != kOk) {
            return status;
        }
    }
    KeyValue* item = cur_block_.add_items();
    item->set_key(key);
    item->set_value(value);
    cur_block_size_ += (key.size() + value.size());
    last_key_ = key;
    return kOk;
}

Status SortFileHdfsWriter::FlushIdxBlock() {
    std::string raw_buf;
    bool ret = idx_block_.SerializeToString(&raw_buf);
    if (!ret) {
        LOG(WARNING, "serialize index fail");
        return kUnKnown;
    }
    int64_t offset = hdfsTell(fs_, fd_);
    if (offset == -1) {
        LOG(WARNING, "get offset fail");
        return kWriteHdfsFileFail;
    }
    int32_t block_size = raw_buf.size();
    int32_t h_ret = hdfsWrite(fs_, fd_, (void*)&block_size, sizeof(int32_t));
    if (h_ret != sizeof(int32_t) ) {
        LOG(WARNING, "write index size fail");
        return kWriteHdfsFileFail;
    }
    h_ret = hdfsWrite(fs_, fd_, (void*)raw_buf.data(), raw_buf.size());
    if (h_ret != (int32_t)raw_buf.size()) {
        LOG(WARNING, "wirte index block fail");
        return kWriteHdfsFileFail;
    }
    h_ret = hdfsWrite(fs_, fd_, (void*)&offset, sizeof(int64_t));
    if (h_ret != sizeof(int64_t)) {
        LOG(WARNING, "write start-offset of index fail");
        return kWriteHdfsFileFail;
    }
    h_ret = hdfsWrite(fs_, fd_, (void*)&sMagicNumber, sizeof(int32_t));
    if (h_ret != sizeof(int32_t) ) {
        LOG(WARNING, "write magic number fail");
        return kWriteHdfsFileFail;
    }  
    return kOk;
}

Status SortFileHdfsWriter::FlushCurBlock() {
    if (cur_block_.items_size() == 0) {
        return kOk;
    }
    std::string raw_buf, compressed_buf;
    bool ret = cur_block_.SerializeToString(&raw_buf);
    if (!ret) {
        LOG(WARNING, "serialize data block fail");
        return kUnKnown;
    }
    snappy::Compress(raw_buf.data(), raw_buf.size(), &compressed_buf);
    int32_t block_size = compressed_buf.size();
    int64_t offset = hdfsTell(fs_, fd_);
    if (offset == -1) {
        LOG(WARNING, "get cur offset fail");
        return kWriteHdfsFileFail;
    }
    int32_t h_ret = hdfsWrite(fs_, fd_, (void*)&block_size, sizeof(int32_t));
    if (h_ret != sizeof(int32_t) ) {
        LOG(WARNING, "write block size fail");
        return kWriteHdfsFileFail;
    }
    h_ret = hdfsWrite(fs_, fd_, (void*)compressed_buf.data(), compressed_buf.size());
    if (h_ret != (int32_t)compressed_buf.size() ) {
        LOG(WARNING, "write data block fail");
        return kWriteHdfsFileFail;
    }
    KeyOffset* item = idx_block_.add_items();
    item->set_key(cur_block_.items(0).key());
    item->set_offset(offset);
    cur_block_.Clear();
    cur_block_size_ = 0;
    return kOk;
}

Status SortFileHdfsWriter::Close() {
    Status status = FlushCurBlock();
    if (status != kOk) {
        return status;
    }
    status = FlushIdxBlock();
    if (status != kOk) {
        return status;
    }
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

}
}
