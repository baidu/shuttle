#include "fileformat.h"

#include <assert.h>
#include <snappy.h>
#include "logging.h"
#include "proto/sortfile.pb.h"

namespace baidu {
namespace shuttle {

/*
 * Sortfile is an internal compressed binary file format, the file format is as following
 * +---------------------------------------+
 * | Block size |  Data Block(compressed)  |
 * +---------------------------------------+
 * |                  ...                  |
 * +---------------------------------------+
 * | Block size | Index Block(compressed)  |
 * +---------------------------------------+
 * |   Index offset   |    Magic number    |
 * +---------------------------------------+
 * Index block is composed by first entries of every data block, as content of block
 * Magic number is to check the completion of a file
 */

class SortFile : public FormattedFile {
public:
    SortFile(File* fp) : fp_(fp), status_(kOk), cur_block_offset_((size_t)-1) { }
    virtual ~SortFile() {
        delete fp_;
    }

    // Before reading a record, Locate() must be called first
    virtual bool ReadRecord(std::string& key, std::string& value);
    /*
     * WriteRecord needs that caller provides sorted KV data, this is only
     *   the file format and do not handle sorting
     */
    virtual bool WriteRecord(const std::string& key, const std::string& value);
    virtual bool Locate(const std::string& key);
    virtual bool Seek(int64_t /*offset*/) {
        // TODO not implement, not qualified to be input file
        return false;
    }
    virtual int64_t Tell() {
        // TODO not implement, not qualified to be input file
        return -1;
    }

    virtual bool Open(const std::string& path, OpenMode mode, const File::Param& param);
    virtual bool Close();

    virtual Status Error() {
        return status_;
    }

    virtual std::string GetFileName() {
        return path_;
    }
    virtual int64_t GetSize();

    virtual bool BuildRecord(const std::string& key, const std::string& value,
            std::string& record);

    static const size_t BLOCK_SIZE = (64 << 10);
    static const int32_t MAX_INDEX_SIZE = 10000;
    static const int32_t MAGIC_NUMBER = 0x55aa;
private:
    // ----- Methods for reading -----
    bool LoadIndexBlock(IndexBlock& index);
    bool LoadDataBlock(DataBlock& block);

    // ----- Methods for writing -----
    bool FlushCurBlock();
    bool FlushIdxBlock();
    bool WriteSerializedBlock(const std::string& block);
    // Index block must fit in memory. This method is to keep index block in proper size
    bool MakeIndexSparse();

private:
    // Non-Nullpointer ensured
    File* fp_;
    OpenMode mode_;
    std::string path_;
    Status status_;

    // ----- Members for reading -----
    DataBlock cur_block_;
    size_t cur_block_offset_;
    int64_t idx_offset_;

    // ----- Members for writing -----
    std::string last_key_;
    IndexBlock idx_block_;
    // DataBlock cur_block_;
    // int32_t cur_block_offset_;
};

bool SortFile::ReadRecord(std::string& key, std::string& value) {
    if (mode_ == kWriteFile) {
        LOG(WARNING, "not supported to read a write-only file: %s", path_.c_str());
        status_ = kReadFileFail;
        return false;
    }
    if (cur_block_offset_ >= static_cast<size_t>(cur_block_.items_size())) {
        if (!LoadDataBlock(cur_block_)) {
            LOG(DEBUG, "unable to load next block, maybe meet EOF or an error");
            return false;
        }
        cur_block_offset_ = 0;
    }
    key = cur_block_.items(cur_block_offset_).key();
    value = cur_block_.items(cur_block_offset_).value();
    ++cur_block_offset_;
    status_ = kOk;
    return true;
}

bool SortFile::WriteRecord(const std::string& key, const std::string& value) {
    if (mode_ == kReadFile) {
        LOG(WARNING, "try to write to read-only file: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }
    if (key < last_key_) {
        LOG(WARNING, "try to put an un-order key: %s, last: %s",
                key.c_str(), last_key_.c_str());
        status_ = kInvalidArg;
        return false;
    }
    KeyValue* item = cur_block_.add_items();
    item->set_key(key);
    item->set_value(value);
    cur_block_offset_ += (key.size() + value.size());
    last_key_ = key;
    if (cur_block_offset_ >= BLOCK_SIZE) {
        return FlushCurBlock();
    }
    status_ = kOk;
    return true;
}

bool SortFile::Locate(const std::string& key) {
    IndexBlock idx_block;
    LOG(INFO, "try to load index of: %s", path_.c_str());
    if (!LoadIndexBlock(idx_block)) {
        LOG(WARNING, "fail to load index block: %s", path_.c_str());
        return false;
    }

    // Bi-search to location the key in index block
    int low = 0;
    int high = idx_block.items_size() - 1;
    if (low > high || (key < idx_block.items(low).key() && !key.empty())) {
        LOG(WARNING, "key `%s' does not exist in index block: %s", key.c_str(), path_.c_str());
        status_ = kInvalidArg;
        return false;
    }
    while (low < high) {
        int mid = (low + high) / 2;
        const std::string& mid_key = idx_block.items(mid).key();
        if (mid_key < key) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    // Get block containing the key
    const std::string& bound_key = idx_block.items(low).key();
    int64_t offset = 0;
    if (bound_key < key) {
        offset = idx_block.items(low).offset();
    } else {
        if (low > 0) {
            offset = idx_block.items(low - 1).offset();
        } else {
            offset = idx_block.items(0).offset();
        }
    }

    if (!fp_->Seek(offset)) {
        LOG(WARNING, "fail to seek the data block at %ld: %s", offset, path_.c_str());
        status_ = kReadFileFail;
        return false;
    }
    // Force ReadRecord to read a new block since (size_t)-1 beats size of any data block
    cur_block_offset_ = (size_t)-1;
    status_ = kOk;
    return true;
}

bool SortFile::Open(const std::string& path, OpenMode mode, const File::Param& param) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    mode_ = mode;
    bool ok = fp_->Open(path, mode, param);
    // Force ReadRecord to read a new block since (size_t)-1 beats size of any data block
    cur_block_offset_ = (size_t)-1;
    status_ = ok ? kOk : kReadFileFail;
    return ok;
}

bool SortFile::Close() {
    LOG(INFO, "try to close: %s", path_.c_str());
    if (mode_ == kWriteFile) {
        if (!FlushCurBlock()) {
            return false;
        }
        if (!FlushIdxBlock()) {
            return false;
        }
    }
    bool ok = fp_->Close();
    status_ = ok ? kOk : kCloseFileFail;
    return ok;
}

int64_t SortFile::GetSize() {
    return fp_->GetSize();
}

bool SortFile::LoadIndexBlock(IndexBlock& index) {
    int64_t file_size = fp_->GetSize();
    int64_t index_offset = 0;
    int32_t magic_number = 0;
    int32_t index_size = 0;
    int32_t span = sizeof(magic_number) + sizeof(index_offset);
    if (file_size <= 0 || !fp_->Seek(file_size - span)) {
        LOG(WARNING, "fail to seek the foot of %s", path_.c_str());
        status_ = kOpenFileFail;
        return false;
    }
    if (fp_->ReadAll(&index_offset, sizeof(index_offset)) != sizeof(index_offset)) {
        LOG(WARNING, "fail to read index offset: %s", path_.c_str());
        status_ = kOpenFileFail;
        return false;
    }
    if (fp_->ReadAll(&magic_number, sizeof(magic_number)) != sizeof(magic_number)) {
        LOG(WARNING, "fail to read index magic: %s", path_.c_str());
        status_ = kBadMagic;
        return false;
    }
    if (!fp_->Seek(index_offset)) {
        LOG(WARNING, "fail to seek the beginning of index offset at %ld: %s",
                index_offset, path_.c_str());
        status_ = kOpenFileFail;
        return false;
    }
    idx_offset_ = index_offset;
    if (fp_->ReadAll(&index_size, sizeof(index_size)) != sizeof(index_size)) {
        LOG(WARNING, "fail to read size of index: %s", path_.c_str());
        status_ = kOpenFileFail;
        return false;
    }
    if (index_size == 0) {
        LOG(INFO, "empty index block: %s", path_.c_str());
        status_ = kOk;
        return true;
    }
    char* raw_buf = new char[index_size];
    if (fp_->ReadAll(raw_buf, index_size) < static_cast<size_t>(index_size)) {
        LOG(WARNING, "fail to read index block: %s", path_.c_str());
        delete[] raw_buf;
        status_ = kReadFileFail;
        return false;
    }
    std::string temp_buf;
    snappy::Uncompress(raw_buf, index_size, &temp_buf);
    delete[] raw_buf;
    if (!index.ParseFromString(temp_buf)) {
        LOG(WARNING, "fail to unserialize index block: %s", path_.c_str());
        status_ = kUnKnown;
        return false;
    }
    status_ = kOk;
    return true;
}

bool SortFile::LoadDataBlock(DataBlock& block) {
    int32_t block_size = 0;
    if (fp_->ReadAll(&block_size, sizeof(block_size)) != sizeof(block_size)) {
        LOG(WARNING, "fail to read block size: %s", path_.c_str());
        status_ = kReadFileFail;
        return false;
    }
    if (block_size == 0) {
        LOG(INFO, "empty data block: %s", path_.c_str());
        status_ = kNoMore;
        return false;
    }
    if (fp_->Tell() + block_size > idx_offset_) {
        LOG(INFO, "no more data block: %s", path_.c_str());
        status_ = kNoMore;
        return false;
    }
    char* raw_buf = new char[block_size];
    if (fp_->ReadAll(raw_buf, block_size) < static_cast<size_t>(block_size)) {
        LOG(WARNING, "fail to read data block: %s", path_.c_str());
        delete[] raw_buf;
        status_ = kReadFileFail;
        return false;
    }
    std::string temp_buf;
    snappy::Uncompress(raw_buf, block_size, &temp_buf);
    delete[] raw_buf;
    if (!block.ParseFromString(temp_buf)) {
        LOG(WARNING, "bad format block: %s", path_.c_str());
        status_ = kUnKnown;
        return false;
    }
    status_ = kOk;
    return true;
}

bool SortFile::FlushCurBlock() {
    if (cur_block_.items_size() == 0) {
        status_ = kOk;
        return true;
    }
    std::string serialized;
    if (!cur_block_.SerializeToString(&serialized)) {
        LOG(WARNING, "fail to serialize data block: %s", path_.c_str());
        status_ = kUnKnown;
        return false;
    }

    int64_t offset = fp_->Tell();
    if (offset == -1) {
        LOG(WARNING, "fail to get current offset: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }

    if (!WriteSerializedBlock(serialized)) {
        return false;
    }

    KeyOffset* item = idx_block_.add_items();
    item->set_key(cur_block_.items(0).key());
    item->set_offset(offset);
    cur_block_.Clear();
    cur_block_offset_ = 0;
    if (idx_block_.items_size() >= MAX_INDEX_SIZE) {
        return MakeIndexSparse();
    }
    status_ = kOk;
    return true;
}

bool SortFile::FlushIdxBlock() {
    std::string serialized;
    if (!idx_block_.SerializeToString(&serialized)) {
        LOG(WARNING, "fail to serialize index block: %s", path_.c_str());
        status_ = kUnKnown;
        return false;
    }

    int64_t offset = fp_->Tell();
    if (offset == -1) {
        LOG(WARNING, "fail to get current offset: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }

    if (!WriteSerializedBlock(serialized)) {
        return false;
    }

    if (!fp_->WriteAll(&offset, sizeof(offset))) {
        LOG(WARNING, "fail to write index offset: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }

    int32_t magic_number = MAGIC_NUMBER;
    if (!fp_->WriteAll(&magic_number, sizeof(magic_number))) {
        LOG(WARNING, "fail to write magic number: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }
    status_ = kOk;
    return true;
}

bool SortFile::WriteSerializedBlock(const std::string& block) {
    // Compress
    std::string compressed_buf;
    snappy::Compress(block.data(), block.size(), &compressed_buf);
    int32_t block_size = compressed_buf.size();
    LOG(DEBUG, "current block_size: %ld", block_size);

    // Write data
    if (!fp_->WriteAll(&block_size, sizeof(block_size))) {
        LOG(WARNING, "fail to write block size: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }
    if (!fp_->WriteAll(compressed_buf.data(), compressed_buf.size())) {
        LOG(WARNING, "fail to write data block: %s", path_.c_str());
        status_ = kWriteFileFail;
        return false;
    }
    return true;
}

bool SortFile::MakeIndexSparse() {
    IndexBlock temp_index;
    temp_index.Swap(&idx_block_);
    assert(idx_block_.items_size() == 0);
    // For every two item remove one item to reduce size
    for (int i = 0; i < temp_index.items_size(); i += 2) {
        KeyOffset* item = idx_block_.add_items();
        item->CopyFrom(temp_index.items(i));
    }
    status_ = kOk;
    return true;
}

bool SortFile::BuildRecord(const std::string& key, const std::string& value,
        std::string& record) {
    int32_t key_len = key.size();
    int32_t value_len = value.size();
    record.assign((const char*)(&key_len), sizeof(key_len));
    record.append(key);
    record.append((const char*)(&value_len), sizeof(value_len));
    record.append(value);
    status_ = kOk;
    return true;
}

namespace factory {

FormattedFile* GetSortFile(File* fp) {
    return new SortFile(fp);
}

} // namespace factory

} // namespace shuttle
} // namespace baidu

