#include "fileformat.h"

#include <string.h>
#include <assert.h>
#include "logging.h"

namespace baidu {
namespace shuttle {

class PlainTextFile : public FormattedFile {
public:
    PlainTextFile(File* fp) : fp_(fp) { }
    virtual ~PlainTextFile() {
        delete fp_;
    }

    virtual bool ReadRecord(std::string& key, std::string& value);
    virtual bool WriteRecord(const std::string& key, const std::string& value);
    virtual bool Locate(const std::string& /*key*/) {
        // TODO not implement, not qualified to be internal sorted file
        return false;
    }
    virtual bool Seek(int64_t offset);
    virtual int64_t Tell();

    virtual bool Open(const std::string& path, OpenMode mode, const File::Param& param);
    virtual bool Close();

    virtual Status Error() {
        return status_;
    }

    virtual std::string GetFileName();

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
            for (size_t i = head_; i < data_.size(); ++i) {
                if (data_[i] == '\n') {
                    line.assign(data_, head_, i - head_);
                    head_ = i + 1;
                    return true;
                }
            }
            // Clean the buffer when there is no full line left
            if (head_ > 0) {
                data_.erase(0, head_);
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
    // Non-Nullpointer ensured
    File* fp_;
    LineBuffer buf_;
    Status status_;
};

bool PlainTextFile::ReadRecord(std::string& key, std::string& value) {
    if (!buf_.ReadLine(value)) {
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
                buf_.GetRemain(value);
                key = "";
                status_ = kOk;
                return true;
            } else {
                status_ = kNoMore;
                return false;
            }
        } else if (ret < 0) {
            status_ = kReadFileFail;
            return false;
        } else {
            // After all the check this time has to be correct
            assert(buf_.ReadLine(value));
        }
    }
    key = "";
    status_ = kOk;
    return true;
}

bool PlainTextFile::WriteRecord(const std::string& /*key*/, const std::string& value) {
    bool ok = fp_->WriteAll(value.data(), value.size());
    status_ = ok ? kOk : kWriteFileFail;
    return ok;
}

bool PlainTextFile::Seek(int64_t offset) {
    char prev_byte = 0;
    if (offset > 0) {
        if (fp_->Seek(offset - 1)) {
            LOG(WARNING, "seek to %ld fail", offset - 1);
            status_ = kReadFileFail;
            return false;
        }
        if (!fp_->Read((void*)&prev_byte, sizeof(prev_byte))) {
            LOG(WARNING, "read prev byte fail");
            status_ = kReadFileFail;
            return false;
        }
        if (!fp_->Seek(offset)) {
            LOG(WARNING, "seek to %ld fail", offset);
            status_ = kReadFileFail;
            return false;
        }
    }
    // Throw out first incomplete line
    if (prev_byte != '\n') {
        std::string key, value;
        ReadRecord(key, value);
    }
    status_ = kOk;
    return true;
}

int64_t PlainTextFile::Tell() {
    return fp_->Tell();
}

bool PlainTextFile::Open(const std::string& path, OpenMode mode, const File::Param& param) {
    bool ok = fp_->Open(path, mode, param);
    status_ = ok ? kOk : kReadFileFail;
    return ok;
}

bool PlainTextFile::Close() {
    bool ok = fp_->Close();
    status_ = ok ? kOk : kCloseFileFail;
    return ok;
}

std::string PlainTextFile::GetFileName() {
    return fp_->GetFileName();
}

bool PlainTextFile::BuildRecord(const std::string& /*key*/, const std::string& value,
        std::string& record) {
    record = value;
    status_ = kOk;
    return true;
}

namespace factory {

FormattedFile* GetPlainTextFile(File* fp) {
    return new PlainTextFile(fp);
}

} // namespace factory

} // namespace shuttle
} // namespace baidu

