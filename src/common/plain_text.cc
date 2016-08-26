#include "fileformat.h"

#include <string.h>
#include <assert.h>
#include "logging.h"

namespace baidu {
namespace shuttle {

class PlainTextFile : public FormattedFile {
public:
    PlainTextFile();
    virtual ~PlainTextFile();
    virtual bool ReadRecord(std::string& record);
    virtual bool WriteRecord(const std::string& record);
    virtual bool Locate(const std::string& key) {
        // TODO not implement, not qualified to be internal sorted file
        return false;
    }
    virtual bool Seek(int64_t offset);
    virtual int64_t Tell();

    virtual bool Open(const std::string& path, OpenMode mode, const Param& param);
    virtual bool Close();

    virtual bool BuildRecord(const std::string& key, const std::string& value,
            std::string& record);

private:
    /*
     * Line Buffer is used to temporary store the block of file
     *   and provides line reading intefaces
     */
    class LineBuffer {
    public:
        LineBuffer() : head_(0) { }
        void Reset() {
            data_.erase();
            head_ = 0;
        }
        bool ReadLine(std::string& line) {
            if (head_ == data_.size()) {
                return false;
            }
            for (size_t i = head_; i < data.size(); ++i) {
                if (data_[i] == '\n') {
                    line.assign(data_, head_, i - head_);
                    head_ = i + 1;
                    return true;
                }
            }
            // Clean the buffer when there is no full line left
            if (head_ > 0) {
                data.erase(0, head_);
                head_ = 0;
            }
            return false;
        }
        void Append(const char* new_data, size_t len) {
            data_.append(new_data, len);
        }
        size_t Size() {
            return data_.size() - head_;
        }
        // GetRemain is used when there's no EOL in the end of file
        void GetRemain(std::string& data) {
            data.assign(data_, head_, data_.size() - head_);
        }
    private:
        std::string data_;
        size_t head_;
    };

private:
    File* fp_;
    LineBuffer buf_;
};

bool PlainTextFile::ReadRecord(std::string& record) {
    if (!buf_.ReadLine(record)) {
        // Read 40kBi every time from file
        int size = 40960;
        char* buf = new char[size];
        int ret = 0;
        // Read until get a full line
        while ((ret = fp_->Read(buf, size)) > 0) {
            buf_.Append(buf, ret);
            if (memchr(buf, '\n', ret) != NULL) {
                break;
            }
        }
        delete[] buf;
        if (ret == 0) {
            // Some files do not have trailing blank line, which leads to no EOL in the end
            if (buf_.Size() > 0) {
                buf_.GetRemain(record);
                return true;
            } else {
                return false;
            }
        } else if (ret < 0) {
            return false;
        } else {
            // After all the check this time has to be correct
            assert(buf_.ReadLine(record));
        }
    }
    return true;
}

inline bool PlainTextFile::WriteRecord(const std::string& record) {
    return fp_->WriteAll(record.data(), record.size());
}

bool PlainTextFile::Seek(int64_t offset) {
    char prev_byte = 0;
    if (offset > 0) {
        if (fp_->Seek(offset - 1)) {
            LOG(WARNING, "seek to %ld fail", offset - 1);
            return false;
        }
        if (!fp_->Read((void*)&prev_byte, sizeof(prev_byte))) {
            LOG(WARNING, "read prev byte fail");
            return false;
        }
        if (!fp_->Seek(offset)) {
            LOG(WARNING, "seek to %ld fail", offset);
            return false;
        }
    }
    // Throw out first incomplete line
    if (prev_byte != '\n') {
        std::string temp;
        ReadRecord(temp);
    }
    return true;
}

inline int64_t PlainTextFile::Tell() {
    return fp_->Tell();
}

inline bool PlainTextFile::Open(const std::string& path, OpenMode mode, const Param& param) {
    if (fp_ == NULL) {
        LOG(WARNING, "empty local file handler");
        return false;
    }
    return fp_->Open(path, mode, param);
}

inline bool PlainTextFile::Close() {
    return fp_->Close();
}

inline bool PlainTextFile::BuildRecord(const std::string& /*key*/, const std::string& value,
        std::string& record) {
    record = value;
    return true;
}

}
}

