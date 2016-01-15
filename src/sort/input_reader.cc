#include "input_reader.h"
#include <string.h>
#include <algorithm>
#include <string>
#include "logging.h"

using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

class LineBuffer {
public:
    LineBuffer() : head_(0) {}
    void Reset() {
        data_.erase();
        head_ = 0;
    }
    void Append(char* new_data, size_t len) {
        data_.append(new_data, len);
    }
    void FillRemain(std::string* line) {
        line->assign(data_, head_, data_.size() - head_);
    }
    bool ReadLine(std::string* line) {
        if (head_ == data_.size()) {
            return false;
        }
        for (size_t i = head_; i < data_.size(); i++) {
            if (data_[i] == '\n') {
                line->assign(data_, head_, i - head_);
                head_ = i + 1;
                return true;
            }
        }
        if (head_ > 0) {
            data_.erase(0, head_);
            head_ = 0;
        }
        return false;
    }
    size_t Size() {
        return data_.size() - head_;
    }
private:
    std::string data_;
    size_t head_;
};

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
        const std::string& Record() { return line_;}
        Status Error() {return status_;};
        void SetHasMore(bool has_more) {has_more_ =  has_more;}
        void SetError(Status err) {status_ = err;}
    private:
        bool has_more_;
        Status status_;
        std::string line_;
        TextReader* reader_;
    };

    TextReader(FileSystem* fs) : fs_(fs),
                                 offset_(0), len_(0),
                                 read_bytes_(0),
                                 reach_eof_(false) {}
    virtual ~TextReader() {delete fs_;}
    Status Open(const std::string& path, FileSystem::Param param);
    Iterator* Read(int64_t offset, int64_t len);
    Status Close();
private:
    Status ReadNextLine(std::string* line);    
private:
    FileSystem* fs_;
    LineBuffer buf_;
    int64_t offset_;
    int64_t len_;
    int64_t read_bytes_;
    bool reach_eof_;
};

class SeqFileReader : public InputReader {
public:
    class IteratorImpl : public InputReader::Iterator {
    public:
        IteratorImpl(SeqFileReader* reader) : has_more_(false),
                                              status_(kOk),
                                              reader_(reader) {}
        virtual ~IteratorImpl() {}
        bool Done() { return !has_more_;}
        void Next();
        const std::string& Record() {return record_;};
        Status Error() {return status_;};
        void SetHasMore(bool has_more) {has_more_ =  has_more;}
        void SetError(Status err) {status_ = err;}
    private:
        bool has_more_;
        Status status_;
        std::string record_;
        SeqFileReader* reader_;
    };

    SeqFileReader(InfSeqFile* sf) : sf_(sf), offset_(0), len_(0),
                                    read_bytes_(0),
                                    reach_eof_(false) {}
    virtual ~SeqFileReader() {delete sf_;}
    Status Open(const std::string& path, FileSystem::Param param);
    Iterator* Read(int64_t offset, int64_t len);
    Status Close();
private:
    Status ReadNextKV(std::string* key, std::string* value);
private:
    InfSeqFile* sf_;
    int64_t offset_;
    int64_t len_;
    int64_t read_bytes_;
    bool reach_eof_;
    std::string end_key_;
    std::string end_value_;
    int64_t end_offfset_;
};

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
    buf_.Reset();
    reach_eof_ = false;
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
    //printf("read_bytes: %ld\n", read_bytes_);
    if (read_bytes_ >= len_ || reach_eof_) {
        return kNoMore;
    }
    if (buf_.ReadLine(line)) {
        //read a line success
    } else {
        int one_size = std::min(40960L, len_);
        char* one_buf = (char*)malloc(one_size);;
        int n_ret = 0;
        while ( (n_ret = fs_->Read((void*)one_buf, one_size)) > 0) {
            buf_.Append(one_buf, n_ret);
            if (memchr(one_buf, '\n', n_ret) != NULL) {
                break;
            }
        }
        free(one_buf);
        if (n_ret < 0) {
            return kReadFileFail;
        } else if (n_ret == 0) {
            if (buf_.Size() > 0) { //sometimes, the last line has no EOL
                buf_.FillRemain(line);
                read_bytes_ += line->size();
                reach_eof_ = true;
                return kOk;
            } else {
                return kNoMore;
            }
        } else {
           assert(buf_.ReadLine(line));
        }
    }
    read_bytes_ += (line->size() + 1);
    return kOk;
}


Status SeqFileReader::Open(const std::string& path, FileSystem::Param param) {
    if (!sf_->Open(path, param, kReadFile)) {
        return kOpenFileFail;
    } else {
        return kOk;
    }
}

InputReader::Iterator* SeqFileReader::Read(int64_t offset, int64_t len) {
    offset_ = offset;
    len_ = len;
    IteratorImpl* it = new IteratorImpl(this);
    if (offset_ + len_ >= sf_->GetSize()) {
        end_offfset_ = 0;
    } else if (sf_->Seek(offset_ + len_)) {
        end_offfset_ = sf_->Tell();
        bool eof;
        sf_->ReadNextRecord(&end_key_, &end_value_, &eof);
    }
    if (!sf_->Seek(offset)) {
        it->SetHasMore(false);
        it->SetError(kReadFileFail);
        return it;
    }
    it->Next();
    return it;
}

Status SeqFileReader::Close() {
    if (!sf_->Close()) {
        return kCloseFileFail;
    }
    return kOk;
}

Status SeqFileReader::ReadNextKV(std::string* key, std::string* value) {
    bool eof = false;
    int64_t cur_pos = sf_->Tell();
    //fprintf(stderr, "cur_pos:%ld, offset:%ld\n", cur_pos, offset_);
    if (cur_pos < 0) {
        return kReadFileFail;
    }
    if (sf_->ReadNextRecord(key, value, &eof) ) {
        if (eof) {
            return kNoMore;
        }
        if (cur_pos == end_offfset_) {
            if (*key == end_key_ && *value == end_value_) {
                return kNoMore;
            }
        }
        return kOk;
    } else {
        return kReadFileFail;
    }
}

void SeqFileReader::IteratorImpl::Next() {
    std::string key;
    std::string value;
    Status status;
    status = reader_->ReadNextKV(&key, &value);
    if (status == kOk) {
        int32_t key_len = (int32_t)key.size();
        int32_t value_len = (int32_t)value.size();
        record_.erase();
        record_.append((const char*)(&key_len), sizeof(key_len));
        record_.append(key);
        record_.append((const char*)(&value_len), sizeof(value_len));
        record_.append(value);
        has_more_ = true;
        status_ = kOk;
    } else if (status == kNoMore) {
        has_more_ = false;
        status_ = kNoMore;
    } else {
        has_more_ = false;
        status_ = status;
    }
}

InputReader* InputReader::CreateHdfsTextReader() {
    return new TextReader(FileSystem::CreateInfHdfs());
}

InputReader* InputReader::CreateLocalTextReader() {
    return new TextReader(FileSystem::CreateLocalFs());
}

InputReader* InputReader::CreateSeqFileReader() {
    return new SeqFileReader(new InfSeqFile());
}

} //namepsace shuttle
} //namespace baidu
