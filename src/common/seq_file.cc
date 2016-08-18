#include "fileformat.h"

#include "hdfs.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

class InfSeqFile : public FormattedFile {
public:
    InfSeqFile() { }

    virtual bool ReadRecord(std::string& record);
    virtual bool WriteRecord(const std::string& record);
    virtual bool Seek(int64_t offset);
    virtual int64_t Tell();

    virtual bool Open(const std::string& path, OpenMode mode);
    virtual bool Close();

    virtual bool ParseRecord(const std::string& record, std::string& key, std::string& value);
    virtual bool BuildRecord(const std::string& key, const std::string& value,
            std::string& record);
private:
    hdfsFS fs_;
    SeqFile sf_;
};

bool InfSeqFile::ReadNextRecord(std::sting& record) {
    int key_len, value_len;
    void *raw_key = NULL, *raw_value = NULL;
    int ret = readNextRecordFromSeqFile(fs_, sf_, &raw_key, &key_len, &raw_value, &value_len);
    if (ret != 0 && ret != 1) {
        LOG(WARNING, "fail to read next record");
        return false;
    }
    if (ret == 1) {
        return false;
    }
    key.assign(raw_key, key_len);
    value.assign(raw_value, value_len);
    return true;
}

bool InfSeqFile::WriteRecord(const std::string& record) {
    std::string key, value;
    if (!ParseRecord(record, key, value)) {
        return false;
    }
    int ret = writeRecordIntoSeqFile(fs_, sf_, key.data(), key.size(),
            value.data(), value.size());
    if (ret != 0) {
        LOG(WARNING, "write next record fail");
        return false;
    }
    return true;
}

inline bool InfSeqFile::Seek(int64_t offset) {
    int64_t ret = syncSeqFile(sf_, offset);
    if (ret < 0) {
        LOG(WARNING, "seek to %ld fail", offset);
        return false;
    }
    return true;
}

inline int64_t InfSeqFile::Tell() {
    return getSeqFilePos(sf_);
}

bool InfSeqFile::Open(const std::string& path, OpenMode mode) {
    if (fs_ != NULL) {
        LOG(WARNING, "empty hdfs handler, fail");
        return false;
    }
    if (mode == kReadFile) {
        sf_ = readSequenceFile(fs_, path.c_str());
        if (!sf_) {
            LOG(WARNING, "fail to read: %s", path.c_str());
            return false;
        }
    } else if (mode == kWriteFile) {
        sf_ = writeSequenceFile(fs_, path.c_str(),
                "BLOCK", "org.apache.hadoop.io.compress.LzoCodec");
        if (!sf_) {
            LOG(WARNING, "fail to write: %s", path.c_str());
            return false;
        }
    } else {
        LOG(FATAL, "unknown mode: %d", mode);
    }
    return true;
}

inline bool InfSeqFile::Close() {
    return closeSequenceFile(fs_, sf_) == 0;
}

bool InfSeqFile::ParseRecord(const std::string& record,
        std::string& key, std::string& value) {
    // TODO
    return false;
}

inline bool InfSeqFile::BuildRecord(const std::string& key, const std::string& value,
        std::string& record) {
    size_t key_len = key.size();
    size_t value_len = value.size();
    record.erase();
    record.append((const char*)(&key_len), sizeof(key_len));
    record.append(key);
    record.append((const char*)(&value_len), sizeof(value_len));
    record.append(value);
    return true;
}

}
}

