#ifndef _BAIDU_SHUTTLE_PLAIN_TEXT_H_
#define _BAIDU_SHUTTLE_PLAIN_TEXT_H_
#include "common/fileformat.h"

namespace baidu {
namespace shuttle {

class PlainTextFile : public FormattedFile {
public:
    PlainTextFile(File* fp) : fp_(fp) { }
    virtual ~PlainTextFile() {
        delete fp_;
    }

    /*
     * Plain text is organized by line, the line is considered as value
     * In read interface, the key will be changed to empty string
     * In write interface, the key will be ingnored
     * Either the interface is accept and return data without '\n'
     */
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
    virtual int64_t GetSize();

    virtual std::string BuildRecord(const std::string& key, const std::string& value) {
        return FormattedFile::BuildRecord(kPlainText, key, value);
    }

protected:
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

protected:
    // Non-Nullpointer ensured
    File* fp_;
    LineBuffer buf_;
    Status status_;
};

}
}

#endif

