#include "bistream.h"

#include <cstring>
#include "logging.h"

namespace baidu {
namespace shuttle {

bool Bistream::ReadRecord(std::string& key, std::string& value) {
    bool ok = true;
    do {
        uint32_t key_len = 0;
        uint32_t value_len = 0;
        size_t has_read = 0;
        if (!GetBufferData(&key_len, sizeof(key_len))) {
            continue;
        }
        has_read += sizeof(key_len);
        key.resize(key_len);
        if (!GetBufferData(&key[0], key_len)) {
            head_ -= has_read;
            continue;
        }
        has_read += key_len;
        if (!GetBufferData(&value_len, sizeof(value_len))) {
            head_ -= has_read;
            continue;
        }
        has_read += sizeof(value_len);
        value.resize(value_len);
        if (!GetBufferData(&value[0], value_len)) {
            head_ -= has_read;
            continue;
        }
        ok = true;
        status_ = kOk;
        break;
    } while (ok = LoadBuffer());
    return ok;
}

bool Bistream::WriteRecord(const std::string& key, const std::string& value) {
    std::string record;
    uint32_t key_len = key.size();
    uint32_t value_len = value.size();
    record.append((const char*)&key_len, sizeof(key_len));
    record.append(key);
    record.append((const char*)&value_len, sizeof(value_len));
    record.append(value);
    bool ok = fp_->WriteAll(value.data(), value.size());
    status_ = ok ? kOk : kWriteFileFail;
    return ok;
}

bool Bistream::Seek(int64_t offset) {
    return fp_->Seek(offset);
}

bool Bistream::GetBufferData(void* data, size_t len) {
    if (head_ + len > buf_.size()) {
        return false;
    }
    memcpy(data, buf_.data() + head_, len);
    return true;
}

bool Bistream::LoadBuffer() {
    buf_.erase(0, head_);
    head_ = 0;
    // Read 40kBi at most every time from stream
    size_t size = 40960;
    char* raw = new char[size];
    int32_t ret = fp_->Read(raw, size);
    if (ret <= 0) {
        delete[] raw;
        status_ = kReadFileFail;
        return false;
    }
    buf_.append(raw, ret);
    delete[] raw;
    status_ = kOk;
    return true;
}

}
}

