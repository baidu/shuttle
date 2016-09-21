#ifndef _BAIDU_SHUTTLE_STREAMING_H_
#define _BAIDU_SHUTTLE_STREAMING_H_
#include "common/format/plain_text.h"

namespace baidu {
namespace shuttle {

// Text streaming resembles plain text IO
typedef PlainTextFile TextStream;

/*
 * BinaryStream is a wrapper for plain text stream to read/write binary data
 *   BinaryStream requires a file pointer and change the ownership
 *   Seek and Tell relies on the underlying file, and Locate is not implement
 *   Note that BinaryStream is just designed for binary IO of a plain text stream
 *   So it is not implement for more complex situation
 *   Also BinaryStream is redundent and indepent with BuildRecord
 */
class BinaryStream : public PlainTextFile {
public:
    BinaryStream(File* fp) : PlainTextFile(fp), head_(0) { }
    virtual ~BinaryStream() { }

    virtual bool ReadRecord(std::string& key, std::string& value);
    virtual bool WriteRecord(const std::string& key, const std::string& value);
    virtual bool Seek(int64_t offset);

    virtual std::string BuildRecord(const std::string& key, const std::string& value);
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

