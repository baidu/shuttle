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
    error_ = kOk;
    cur_offset_ = 0;
    start_key_ = start_key;
    end_key_ = end_key;
}

SortFileHdfsReader::IteratorHdfs::~IteratorHdfs() {

}

void SortFileHdfsReader::IteratorHdfs::Init() {
    if (has_more_ && cur_block_.items_size() == 0) {
        //Initiate data for the iterator, locate to the right place
        Status status = reader_->ReadNextRecord(cur_block_);
        if (status != kOk) {
            error_ = status;
            has_more_ = false;
            return;
        }
        while (status == kOk) {
            while(cur_offset_ < cur_block_.items_size() &&
                  cur_block_.items(cur_offset_).key() < start_key_) {
                cur_offset_ ++;
            } //skip the items less than start_key
            if (cur_offset_ >= cur_block_.items_size()) {
                status = reader_->ReadNextRecord(cur_block_);
                //read the next block
            } else {
                break;
            }
        }
        if (status != kOk) {
            error_ = status;
            has_more_ = false;
            return;
        }
        key_ = cur_block_.items(cur_offset_).key();
        value_ =  cur_block_.items(cur_offset_).value();
        if (key_ >= end_key_ && !end_key_.empty()) {
            has_more_ = false;
            return;
        }
    }
}

bool SortFileHdfsReader::IteratorHdfs::Done() {
    return !has_more_;
}

void SortFileHdfsReader::IteratorHdfs::Next() {
    cur_offset_ ++ ;
    if (cur_offset_ >= cur_block_.items_size()) {
        Status status = reader_->ReadNextRecord(cur_block_);
        if (status != kOk) {
            error_ = status;
            has_more_ = false;
            return;
        }
        cur_offset_ = 0;
    }
    if (cur_block_.items(cur_offset_).key() >= end_key_
        && !end_key_.empty()) {
        has_more_ = false;
        return;
    }
    key_ = cur_block_.items(cur_offset_).key();
    value_ = cur_block_.items(cur_offset_).value();
}

const std::string& SortFileHdfsReader::IteratorHdfs::Key() {
    return key_;
}

const std::string& SortFileHdfsReader::IteratorHdfs::Value() {
    return value_;
}

Status SortFileHdfsReader::IteratorHdfs::Error() {
    return error_;
}

void SortFileHdfsReader::IteratorHdfs::SetError(Status status) {
    error_ = status;
}

void SortFileHdfsReader::IteratorHdfs::SetHasMore(bool has_more) {
    has_more_ = has_more;
}

Status SortFileHdfsReader::ReadFull(std::string* result_buf, int32_t len,
                                    bool is_read_data) {
    if (result_buf == NULL || len < 0 ) {
        return kInvalidArg;
    }
    result_buf->reserve(len);
    Status status = kOk;
    int once_buf_size = std::min(40960, len); //at most 40K
    char* buf = (char*)malloc(once_buf_size);
    int n_read = hdfsRead(fs_, fd_, (void*)buf, once_buf_size);

    if (n_read < 0) {
        free(buf);
        LOG(WARNING, "fail to read block of %s", path_.c_str());
        return kReadHdfsFail;
    }
    if (n_read == 0) {
        LOG(WARNING, "read EOF, %s", path_.c_str());
        free(buf);
        return kNoMore;
    }

    if (is_read_data) {
        int64_t cur_offset = hdfsTell(fs_, fd_);
        if (cur_offset > idx_offset_) {
            free(buf);
            LOG(WARNING, "no more data block, %s", path_.c_str());
            return kNoMore;
        }
    }

    result_buf->append(buf, n_read);
    while (result_buf->size() < (size_t)len) {
        n_read = hdfsRead(fs_, fd_, (void*)buf, once_buf_size);
        if (n_read < 0) {
            status = kReadHdfsFail;
            break;
        } else if (n_read == 0) {
            status = kNoMore;
            break;
        } else {
            if (result_buf->size() + n_read > (size_t)len) {
                n_read = len - result_buf->size();
            }
            result_buf->append(buf, n_read);
        }
    }
    free(buf);
    return status;
}

Status SortFileHdfsReader::ReadNextRecord(DataBlock& data_block) {
    int32_t block_size;
    int n_read = hdfsRead(fs_, fd_, (void*)&block_size, sizeof(int32_t));
    if (n_read != sizeof(int32_t)) {
        LOG(WARNING, "fail to read block size, %s", path_.c_str());
        return kReadHdfsFail;
    }
    std::string block_raw, block_uncompress;
    Status status = ReadFull(&block_raw, block_size, true);
    if (status != kOk) {
        return status;
    }
    snappy::Uncompress(block_raw.data(), block_raw.size(), &block_uncompress);
    bool ret = data_block.ParseFromString(block_uncompress);
    if (!ret) {
        LOG(WARNING, "bad format block, %s", path_.c_str());
        return kUnKnown;
    }
    return kOk;
}

Status SortFileHdfsReader::LoadIndexBlock(const std::string& path) {
    hdfsFileInfo* info = NULL;
    info = hdfsGetPathInfo(fs_, path.c_str());
    if (info == NULL) {
        LOG(WARNING, "failed to get info of %s", path.c_str());
        return kOpenHdfsFileFail;
    }
    int64_t file_size = info->mSize;
    hdfsFreeFileInfo(info, 1);
    int32_t magic_number;
    int64_t index_offset;
    int32_t index_size;
    int32_t span = sizeof(int32_t) + sizeof(int64_t);
    if (hdfsSeek(fs_, fd_, file_size - span)) {
        LOG(WARNING, "fail to seek the foot of %s", path.c_str());
        return kOpenHdfsFileFail;
    }
    int n_read = hdfsRead(fs_, fd_, (void*)&index_offset, sizeof(int64_t));
    if (n_read != sizeof(int64_t)) {
        LOG(WARNING, "fail to read index offset, %s, %d", path.c_str(), n_read);
        return kOpenHdfsFileFail;
    }
    n_read = hdfsRead(fs_, fd_, (void*)&magic_number, sizeof(int32_t));
    if (n_read != sizeof(int32_t) || magic_number != sMagicNumber) {
        LOG(WARNING, "fail to read index magic, %s, %d", path.c_str(), magic_number);
        return kBadMagic;
    }
    if (hdfsSeek(fs_, fd_, index_offset)) {
        LOG(WARNING, "fail to seek the start index offset of %s", path.c_str());
        return kOpenHdfsFileFail;
    }
    idx_offset_ = index_offset;
    n_read = hdfsRead(fs_, fd_, (void*)&index_size, sizeof(int32_t));
    if (n_read != sizeof(int32_t)) {
        LOG(WARNING, "fail to read size of index, %s, %d", path.c_str(), n_read);
        return kOpenHdfsFileFail;
    }
    std::string index_raw_buf;
    Status status = ReadFull(&index_raw_buf, index_size);
    if (status != kOk) {
        LOG(WARNING, "read index block fail, %s", Status_Name(status).c_str());
        if (status == kNoMore) { //empty index
            return kOk;
        }
        return status;
    }
    bool ret = idx_block_.ParseFromString(index_raw_buf);
    if (!ret) {
        LOG(WARNING, "unserialize index block fail, %s", path.c_str());
        return kUnKnown;
    }
    //printf("debug: %s\n", idx_block_.DebugString().c_str());
    return kOk;
}

Status SortFileHdfsReader::Open(const std::string& path, Param& param) {
    LOG(INFO, "try to open: %s", path.c_str());
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
    path_ = path;
    fd_ = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, 0, 0, 0);
    if (!fd_) {
        LOG(WARNING, "open %s for write fail", path.c_str());
        return kOpenHdfsFileFail;
    }
    Status status = LoadIndexBlock(path);
    if (status != kOk) {
        return status;
    }
    return kOk;
}

SortFileReader::Iterator* SortFileHdfsReader::Scan(const std::string& start_key, 
                                                   const std::string& end_key) {
    if (start_key > end_key && !end_key.empty()) {
        IteratorHdfs* it = new IteratorHdfs(start_key, end_key, this);
        it->SetHasMore(false);
        it->SetError(kInvalidArg);
        LOG(WARNING, "invalid scan arg: %s to %s", 
            start_key.c_str(), end_key.c_str());
        return it; 
    }
    int low = 0;
    int high = idx_block_.items_size() - 1;

    if (low > high || (end_key <= idx_block_.items(low).key() && !end_key.empty()) ) {
        IteratorHdfs* it = new IteratorHdfs(start_key, end_key, this);
        it->SetHasMore(false);
        return it; //return an empty iterator
    }

    while (low < high) {
        int mid = low + (high - low) / 2;
        const std::string& mid_key = idx_block_.items(mid).key();
        if (mid_key < start_key) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    
    const std::string& bound_key = idx_block_.items(low).key();
    int64_t offset;
    if (bound_key <= start_key) {
        offset = idx_block_.items(low).offset();
    } else {
        if (low > 0) {
            offset = idx_block_.items(low-1).offset();
        } else {
            offset = idx_block_.items(0).offset();
        }
    }
    IteratorHdfs* it = new IteratorHdfs(start_key, end_key, this);
    if (hdfsSeek(fs_, fd_, offset)) {
        LOG(WARNING, "fail to seek the data block at %ld", offset);
        it->SetHasMore(false);
        it->SetError(kReadHdfsFail);
    } else {
        it->SetHasMore(true);
    }
    it->Init();
    return it;
}

Status SortFileHdfsReader::Close() {
    LOG(INFO, "try close file: %s", path_.c_str());
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
