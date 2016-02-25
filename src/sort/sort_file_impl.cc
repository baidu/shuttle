#include "sort_file_impl.h"
#include "logging.h"
#include <snappy.h>

using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

const static int32_t sBlockSize = (64 << 10);
const static int32_t sMagicNumber = 25997;
const static int32_t sMaxIndexSize = 15000;

SortFileReader* SortFileReader::Create(FileType file_type, Status* status) {
    if (file_type == kHdfsFile) {
        *status = kOk;
        return new SortFileReaderImpl(FileSystem::CreateInfHdfs());
    } else if (file_type == kLocalFile) {
        *status = kOk;
        return new SortFileReaderImpl(FileSystem::CreateLocalFs());
    } else {
        *status = kNotImplement;
        return NULL;
    }
}

SortFileWriter* SortFileWriter::Create(FileType file_type, Status* status) {
    if (file_type == kHdfsFile) {
        *status = kOk;
        return new SortFileWriterImpl(FileSystem::CreateInfHdfs());
    } else if (file_type == kLocalFile) {
        *status = kOk;
        return new SortFileWriterImpl(FileSystem::CreateLocalFs());
    } else {
        *status = kNotImplement;
        return NULL;
    }
}

SortFileReaderImpl::IteratorImpl::IteratorImpl(const std::string& start_key,
                                               const std::string& end_key,
                                               SortFileReaderImpl* reader) {
    reader_ = reader;
    error_ = kOk;
    cur_offset_ = 0;
    start_key_ = start_key;
    end_key_ = end_key;
}

SortFileReaderImpl::IteratorImpl::~IteratorImpl() {

}

const std::string SortFileReaderImpl::IteratorImpl::GetFileName() {
    if (reader_) {
        return reader_->path_;
    } else {
        return "";
    }
}

void SortFileReaderImpl::IteratorImpl::Init() {
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
                //printf("%d\n", cur_offset_);
            } //skip the items less than start_key
            if (cur_offset_ >= cur_block_.items_size()) {
                status = reader_->ReadNextRecord(cur_block_);
                cur_offset_ = 0;
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

bool SortFileReaderImpl::IteratorImpl::Done() {
    return !has_more_;
}

void SortFileReaderImpl::IteratorImpl::Next() {
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

const std::string& SortFileReaderImpl::IteratorImpl::Key() {
    return key_;
}

const std::string& SortFileReaderImpl::IteratorImpl::Value() {
    return value_;
}

Status SortFileReaderImpl::IteratorImpl::Error() {
    return error_;
}

void SortFileReaderImpl::IteratorImpl::SetError(Status status) {
    error_ = status;
}

void SortFileReaderImpl::IteratorImpl::SetHasMore(bool has_more) {
    has_more_ = has_more;
}

Status SortFileReaderImpl::ReadFull(std::string* result_buf, int32_t len,
                                    bool is_read_data) {
    if (result_buf == NULL || len < 0 ) {
        return kInvalidArg;
    }
    //LOG(INFO, "cur offset: %ld, need: %d", fs_->Tell(), len);
    result_buf->reserve(len);
    Status status = kOk;
    int once_buf_size = std::min(40960, len); //at most 40K
    char* buf = (char*)malloc(once_buf_size);
    int n_read = fs_->Read((void*)buf, once_buf_size);

    if (n_read < 0) {
        free(buf);
        LOG(WARNING, "fail to read block of %s", path_.c_str());
        return kReadFileFail;
    }
    if (n_read == 0) {
        LOG(WARNING, "read EOF, %s", path_.c_str());
        free(buf);
        return kNoMore;
    }

    if (is_read_data) {
        int64_t cur_offset = fs_->Tell();
        if (cur_offset > idx_offset_) {
            free(buf);
            LOG(WARNING, "no more data block, %s", path_.c_str());
            return kNoMore;
        }
    }

    result_buf->append(buf, n_read);
    while (result_buf->size() < (size_t)len) {
        int try_read_count = std::min((size_t)once_buf_size, len - result_buf->size());
        n_read = fs_->Read((void*)buf, try_read_count);
        if (n_read < 0) {
            status = kReadFileFail;
            break;
        } else if (n_read == 0) {
            status = kNoMore;
            break;
        } else {
            result_buf->append(buf, n_read);
        }
    }
    free(buf);
    //LOG(INFO, "after, cur offset: %ld", fs_->Tell());
    return status;
}

Status SortFileReaderImpl::ReadNextRecord(DataBlock& data_block) {
    int32_t block_size;
    int n_read = fs_->Read((void*)&block_size, sizeof(int32_t));
    //LOG(INFO, "read: %s, block_size: %ld", path_.c_str(), block_size);
    if (n_read != sizeof(int32_t)) {
        LOG(WARNING, "fail to read block size, %s", path_.c_str());
        return kReadFileFail;
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

Status SortFileReaderImpl::LoadIndexBlock(IndexBlock* idx_block) {
    int64_t file_size = fs_->GetSize();
    int32_t magic_number;
    int64_t index_offset;
    int32_t index_size;
    int32_t span = sizeof(int32_t) + sizeof(int64_t);
    if (file_size <= 0 || !fs_->Seek(file_size - span)) {
        LOG(WARNING, "fail to seek the foot of %s", path_.c_str());
        return kOpenFileFail;
    }
    int n_read = fs_->Read((void*)&index_offset, sizeof(int64_t));
    if (n_read != sizeof(int64_t)) {
        LOG(WARNING, "fail to read index offset, %s, %d", path_.c_str(), n_read);
        return kOpenFileFail;
    }
    n_read = fs_->Read((void*)&magic_number, sizeof(int32_t));
    if (n_read != sizeof(int32_t) || magic_number != sMagicNumber) {
        LOG(WARNING, "fail to read index magic, %s, %d", path_.c_str(), magic_number);
        return kBadMagic;
    }
    if (!fs_->Seek(index_offset)) {
        LOG(WARNING, "fail to seek the start index offset of %s at %ld",
            path_.c_str(), index_offset);
        return kOpenFileFail;
    }
    idx_offset_ = index_offset;
    n_read = fs_->Read((void*)&index_size, sizeof(int32_t));
    if (n_read != sizeof(int32_t)) {
        LOG(WARNING, "fail to read size of index, %s, %d", path_.c_str(), n_read);
        return kOpenFileFail;
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
    std::string tmp_buf;
    snappy::Uncompress(index_raw_buf.data(), index_raw_buf.size(), &tmp_buf);
    bool ret = idx_block->ParseFromString(tmp_buf);
    if (!ret) {
        LOG(WARNING, "unserialize index block fail, %s", path_.c_str());
        return kUnKnown;
    }
    //printf("debug: %s\n", idx_block->DebugString().c_str());
    return kOk;
}

Status SortFileReaderImpl::Open(const std::string& path, FileSystem::Param param) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    if (!fs_->Open(path, param, kReadFile)) {
        return kOpenFileFail;
    }
    return kOk;
}

SortFileReader::Iterator* SortFileReaderImpl::Scan(const std::string& start_key, 
                                                   const std::string& end_key) {
    if (start_key > end_key && !end_key.empty()) {
        IteratorImpl* it = new IteratorImpl(start_key, end_key, this);
        it->SetHasMore(false);
        it->SetError(kInvalidArg);
        LOG(WARNING, "invalid scan arg: %s to %s", 
            start_key.c_str(), end_key.c_str());
        return it; 
    }

    IndexBlock idx_block;
    LOG(INFO, "try load index of: %s", path_.c_str());
    Status status = LoadIndexBlock(&idx_block);
    for(int i = 0; i < 3 && status != kOk; i++) {
        idx_block.Clear();
        status = LoadIndexBlock(&idx_block);
        sleep(1);
    }
    if (status != kOk) {
        LOG(WARNING, "faild to load index block, %s", path_.c_str());
        IteratorImpl* it = new IteratorImpl(start_key, end_key, this);
        it->SetHasMore(false);
        it->SetError(kReadFileFail);
        return it;
    }

    int low = 0;
    int high = idx_block.items_size() - 1;

    if (low > high || (end_key <= idx_block.items(low).key() && !end_key.empty()) ) {
        IteratorImpl* it = new IteratorImpl(start_key, end_key, this);
        it->SetHasMore(false);
        return it; //return an empty iterator
    }

    while (low < high) {
        int mid = low + (high - low) / 2;
        const std::string& mid_key = idx_block.items(mid).key();
        if (mid_key < start_key) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    const std::string& bound_key = idx_block.items(low).key();
    int64_t offset;
    if (bound_key < start_key) {
        offset = idx_block.items(low).offset();
    } else {
        if (low > 0) {
            offset = idx_block.items(low-1).offset();
        } else {
            offset = idx_block.items(0).offset();
        }
    }
    IteratorImpl* it = new IteratorImpl(start_key, end_key, this);
    if (!fs_->Seek(offset)) {
        LOG(WARNING, "fail to seek the data block at %ld", offset);
        it->SetHasMore(false);
        it->SetError(kReadFileFail);
    } else {
        it->SetHasMore(true);
    }
    it->Init();
    return it;
}

Status SortFileReaderImpl::Close() {
    LOG(INFO, "try close file: %s", path_.c_str());
    if (!fs_->Close()) {
        return kCloseFileFail;
    }
    return kOk;
}

SortFileWriterImpl::SortFileWriterImpl(FileSystem* fs) : cur_block_size_(0),
                                                         fs_(fs) {

}

Status SortFileWriterImpl::Open(const std::string& path, FileSystem::Param param) {
    if (!fs_->Open(path, param, kWriteFile)) {
        return kOpenFileFail;
    }
    path_ = path;
    return kOk;
}

Status SortFileWriterImpl::Put(const std::string& key, const std::string& value) {
    if (key < last_key_) {
        LOG(WARNING, "try to put a un-ordered key: %s \n last: %s",
            key.c_str(), last_key_.c_str());
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

Status SortFileWriterImpl::FlushIdxBlock() {
    while (idx_block_.items_size() > sMaxIndexSize) {
        MakeIndexSparse();
    }
    std::string raw_buf, tmp_buf;
    bool ret = idx_block_.SerializeToString(&tmp_buf);
    if (!ret) {
        LOG(WARNING, "serialize index fail");
        return kUnKnown;
    }
    snappy::Compress(tmp_buf.data(), tmp_buf.size(), &raw_buf);
    int64_t offset = fs_->Tell();
    if (offset == -1) {
        LOG(WARNING, "get offset fail");
        return kWriteFileFail;
    }
    int32_t block_size = raw_buf.size();
    int32_t h_ret = fs_->Write((void*)&block_size, sizeof(int32_t));
    if (h_ret != sizeof(int32_t) ) {
        LOG(WARNING, "write index size fail");
        return kWriteFileFail;
    }
    h_ret = fs_->Write((void*)raw_buf.data(), raw_buf.size());
    if (h_ret != (int32_t)raw_buf.size()) {
        LOG(WARNING, "wirte index block fail");
        return kWriteFileFail;
    }
    h_ret = fs_->Write((void*)&offset, sizeof(int64_t));
    if (h_ret != sizeof(int64_t)) {
        LOG(WARNING, "write start-offset of index fail");
        return kWriteFileFail;
    }
    h_ret = fs_->Write((void*)&sMagicNumber, sizeof(int32_t));
    if (h_ret != sizeof(int32_t) ) {
        LOG(WARNING, "write magic number fail");
        return kWriteFileFail;
    }  
    return kOk;
}

void SortFileWriterImpl::MakeIndexSparse() {
    IndexBlock tmp_index;
    tmp_index.Swap(&idx_block_);
    assert(idx_block_.items_size() == 0);
    for (int i = 0; i < tmp_index.items_size(); i+=2) {
        KeyOffset* item = idx_block_.add_items();
        item->CopyFrom(tmp_index.items(i));
    }
}

Status SortFileWriterImpl::FlushCurBlock() {
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
    int64_t offset = fs_->Tell();
    if (offset == -1) {
        LOG(WARNING, "get cur offset fail");
        return kWriteFileFail;
    }
    //LOG(INFO, "file:%s, block_size: %ld", path_.c_str(), block_size);
    int32_t h_ret = fs_->Write((void*)&block_size, sizeof(int32_t));
    if (h_ret != sizeof(int32_t) ) {
        LOG(WARNING, "write block size fail");
        return kWriteFileFail;
    }
    h_ret = fs_->Write((void*)compressed_buf.data(), compressed_buf.size());
    if (h_ret != (int32_t)compressed_buf.size() ) {
        LOG(WARNING, "write data block fail");
        return kWriteFileFail;
    }

    KeyOffset* item = idx_block_.add_items();
    item->set_key(cur_block_.items(0).key());
    item->set_offset(offset);

    cur_block_.Clear();
    cur_block_size_ = 0;
    return kOk;
}

Status SortFileWriterImpl::Close() {
    Status status = FlushCurBlock();
    if (status != kOk) {
        return status;
    }
    status = FlushIdxBlock();
    if (status != kOk) {
        return status;
    }
    if (!fs_->Close()) {
        return kCloseFileFail;
    }
    return kOk;
}

}
}
