#include "scanner.h"

namespace baidu {
namespace shuttle {

const std::string Scanner::SCAN_ALL_KEY = "";
const std::string Scanner::SCAN_KEY_BEGINNING = "";

class InputReader : public Scanner {
public:
    InputReader(FormattedFile* fp) : fp_(fp) { }
    virtual ~InputReader() { }

    virtual Iterator* Scan(int64_t offset, int64_t len);
    virtual std::string GetFileName() {
        return fp_->GetFileName();
    }

    class Iterator : public Scanner::Iterator {
    public:
        Iterator(FormattedFile* fp, int64_t len) : fp_(fp),
                status_(kOk), len_(len), read_bytes_(0) { }
        virtual ~Iterator() { }

        virtual bool Done() {
            return status_ != kOk;
        }
        virtual void Next();
        virtual Status Error() {
            return status_;
        }
        virtual const std::string& Key() {
            return key_;
        }
        virtual const std::string& Value() {
            return value_;
        }
        virtual std::string GetFileName() {
            return fp_->GetFileName();
        }

    private:
        // Only use the pointer for reading
        FormattedFile* fp_;
        Status status_;
        std::string key_;
        std::string value_;
        int64_t len_;
        int64_t read_bytes_;
    };

private:
    // Non-Nullpointer garanteed
    FormattedFile* fp_;
};

class InternalReader : public Scanner {
public:
    InternalReader(FormattedFile* fp) : fp_(fp) { }
    virtual ~InternalReader() { }

    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual std::string GetFileName() {
        return fp_->GetFileName();
    }

    class Iterator : public Scanner::Iterator {
    public:
        Iterator(FormattedFile* fp, const std::string& end_key) :
                fp_(fp), status_(kOk), end_(end_key), scan_all_(end_ == SCAN_ALL_KEY) { }
        virtual ~Iterator() { }

        virtual bool Done() {
            return status_ != kOk;
        }
        virtual void Next();
        virtual Status Error() {
            return status_;
        }
        virtual const std::string& Key() {
            return key_;
        }
        virtual const std::string& Value() {
            return value_;
        }
        virtual std::string GetFileName() {
            return fp_->GetFileName();
        }

    private:
        // Only use the pointer for reading
        FormattedFile* fp_;
        Status status_;
        std::string key_;
        std::string value_;
        std::string end_;
        bool scan_all_;
    };

private:
    // Non-Nullpointer garanteed
    FormattedFile* fp_;
};

Scanner* Scanner::Get(FormattedFile* fp, ScannerType type) {
    return fp == NULL ? NULL : (
            type == kInputScanner ? static_cast<Scanner*>(new InputReader(fp)) : (
                type == kInternalScanner ? static_cast<Scanner*>(new InternalReader(fp)) :
                    NULL
            )
        );
}

Scanner::Iterator* InputReader::Scan(int64_t offset, int64_t len) {
    if (len == Scanner::SCAN_MAX_LEN) {
        len = fp_->GetSize();
    }
    if (!fp_->Seek(offset)) {
        return NULL;
    }
    InputReader::Iterator* it = new InputReader::Iterator(fp_, len);
    it->Next();
    return it;
}

void InputReader::Iterator::Next() {
    if (read_bytes_ > len_) {
        status_ = kNoMore;
        return;
    }
    if (fp_->ReadRecord(key_, value_)) {
        read_bytes_ += value_.size() + 1;
    }
    status_ = fp_->Error();
}

Scanner::Iterator* InternalReader::Scan(const std::string& start_key,
        const std::string& end_key) {
    if (!fp_->Locate(start_key)) {
        return NULL;
    }
    InternalReader::Iterator* it = new InternalReader::Iterator(fp_, end_key);
    it->Next();
    return it;
}

void InternalReader::Iterator::Next() {
    if (status_ == kNoMore) {
        return;
    }
    if (!scan_all_ && key_ >= end_) {
        status_ = kNoMore;
        return;
    }
    fp_->ReadRecord(key_, value_);
    status_ = fp_->Error();
}

}
}

