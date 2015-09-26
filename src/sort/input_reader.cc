#include "input_reader.h"
#include <string.h>
#include <algorithm>
#include <string>
#include "logging.h"

using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

class TextReader : public InputReader {
public:
    class IteratorImpl : public InputReader::Iterator {
    public:
        IteratorImpl(TextReader* reader) : has_more_(false), 
                                           status_(kOk),
                                           reader_(reader) {}
        virtual ~IteratorImpl() {}
        bool Done() { return !has_more_;}
        void Next();
        const std::string& Line() { return line_;}
        Status Error() {return status_;};
        void SetHasMore(bool has_more) {has_more_ =  has_more;}
        void SetError(Status err) {status_ = err;}
    private:
        bool has_more_;
        Status status_;
        std::string line_;
        TextReader* reader_;
    };

    TextReader(FileSystem* fs) : fs_(fs), read_bytes_(0) {}
    virtual ~TextReader() {delete fs_;}
    Status Open(const std::string& path, FileSystem::Param param);
    Iterator* Read(int64_t offset, int64_t len);
    Status Close();
private:
    Status ReadNextLine(std::string* line);    
private:
    FileSystem* fs_;
    std::string buf_;
    int64_t offset_;
    int64_t len_;
    int64_t read_bytes_;
};

InputReader* InputReader::CreateHdfsTextReader() {
    return new TextReader(FileSystem::CreateInfHdfs());
}

void TextReader::IteratorImpl::Next() {
    Status status = reader_->ReadNextLine(&line_);
    if (status != kOk) {
        has_more_ = false;
    } else {
        has_more_ = true;
    }
    status_ = status;
}

Status TextReader::Open(const std::string& path, FileSystem::Param param) {
    if (!fs_->Open(path, param, kReadFile)) {
        return kOpenFileFail;
    }
    return kOk;
}

InputReader::Iterator* TextReader::Read(int64_t offset, int64_t len) {
    offset_ = offset;
    len_ = len;
    read_bytes_ = 0;
    buf_.erase();
    IteratorImpl* it = new IteratorImpl(this);
    char byte_prev;
    if (offset > 0) {
        //need to skip the first in-complete line;
        if (!fs_->Seek(offset - 1)) {
            LOG(WARNING, "seek to %ld fail", offset - 1);
            it->SetHasMore(false);
            it->SetError(kReadFileFail);
            return it;
        }
        if (!fs_->Read((void*)&byte_prev, 1)) {
            LOG(WARNING, "read prev byte fail");
            it->SetHasMore(false);
            it->SetError(kReadFileFail);
            return it;
        }
    }
    if (!fs_->Seek(offset)) {
        LOG(WARNING, "seek to %ld fail", offset);
        it->SetHasMore(false);
        it->SetError(kReadFileFail);
        return it;
    }
    it->Next();
    if (it->Error() == kOk && offset > 0) {
        if (byte_prev != '\n') {
            it->Next(); //jump the first line
        }
    }
    return it;
}

Status TextReader::Close() {
    if(!fs_->Close()) {
        return kCloseFileFail;
    }
    return kOk;
}

Status TextReader::ReadNextLine(std::string* line) {
    assert(line);
    size_t pos = 0;
    if (read_bytes_ > len_) {
        return kNoMore;
    }
    if ( (pos = buf_.find("\n")) != std::string::npos) {
        *line = buf_.substr(0, pos);
        buf_ = buf_.substr(pos + 1);
    } else {
        int one_size = std::min(40960L, len_);
        char* one_buf = (char*)malloc(one_size);;
        int n_ret = 0;
        while ( (n_ret = fs_->Read((void*)one_buf, one_size)) > 0) {
            read_bytes_ += n_ret;
            buf_.append(one_buf, n_ret);
            if (memchr(one_buf, '\n', n_ret) != NULL) {
                break;
            }
        }
        free(one_buf);
        if (n_ret < 0) {
            return kReadFileFail;
        } else if (n_ret == 0) {
            return kNoMore;
        } else {
            pos = buf_.find("\n");
            assert(pos != std::string::npos);
            *line = buf_.substr(0, pos);
            buf_ = buf_.substr(pos + 1);
        }
    }
    return kOk;
}

} //namepsace shuttle
} //namespace baidu
