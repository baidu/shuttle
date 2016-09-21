#include "plain_text.h"

#include <string.h>
#include <assert.h>
#include "logging.h"

namespace baidu {
namespace shuttle {

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
    const std::string& line = value + '\n';
    bool ok = fp_->WriteAll(line.data(), line.size());
    status_ = ok ? kOk : kWriteFileFail;
    return ok;
}

bool PlainTextFile::Seek(int64_t offset) {
    char prev_byte = '\n';
    if (offset > 0) {
        if (!fp_->Seek(offset - 1)) {
            LOG(WARNING, "seek to %ld fail", offset - 1);
            status_ = kReadFileFail;
            return false;
        }
        if (!fp_->Read((void*)&prev_byte, sizeof(prev_byte))) {
            LOG(WARNING, "read prev byte fail");
            status_ = kReadFileFail;
            return false;
        }
    }
    if (!fp_->Seek(offset)) {
        LOG(WARNING, "seek to %ld fail", offset);
        status_ = kReadFileFail;
        return false;
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

int64_t PlainTextFile::GetSize() {
    return fp_->GetSize();
}

}
}

