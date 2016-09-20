#ifndef _BAIDU_SHUTTLE_BISTREAM_H_
#define _BAIDU_SHUTTLE_BISTREAM_H_
#include "common/format/plain_text.h"

namespace baidu {
namespace shuttle {

/*
 * Bistream is a wrapper for plain text stream to read/write binary data
 *   Bistream requires a file pointer and change the ownership
 *   Seek and Tell relies on the underlying file, and Locate is not implement
 *   Note that Bistream is just designed for binary IO of a plain text stream
 *   So it is not implement for more complex situation
 *   Also Bistream is indepent with BuildRecord
 */
class Bistream : public PlainTextFile {
public:
    Bistream(File* fp) : PlainTextFile(fp), head_(0) { }
    virtual ~Bistream() { }

    virtual bool ReadRecord(std::string& key, std::string& value);
    virtual bool WriteRecord(const std::string& key, const std::string& value);
    virtual bool Seek(int64_t offset);
private:
    bool GetBufferData(void* data, size_t len);
    bool LoadBuffer();
private:
    std::string buf_;
    size_t head_;
    // File* fp_;
    // Status status_;
};

}
}

#endif

