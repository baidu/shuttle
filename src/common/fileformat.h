#ifndef _BAIDU_SHUTTLE_FILEFORMAT_H_
#define _BAIDU_SHUTTLE_FILEFORMAT_H_
#include <stdint.h>
#include "file.h"

namespace baidu {
namespace shuttle {

enum FileFormat {
    kPlainText = 1,
    kInfSeqFile = 2,
    kInternalSortedFile = 3
};

class FormattedFile {
public:
    // Record here is automatically aligned, which means if specified offset is in the middle
    //   of a record, ReadRecord will ignore the rest of current record and
    //   get next complete record
    virtual bool ReadRecord(std::string& record) = 0;
    virtual bool WriteRecord(const std::string& record) = 0;
    virtual bool Seek(int64_t offset) = 0;
    virtual int64_t Tell();

    virtual bool Open(const std::string& path, OpenMode mode) = 0;
    virtual bool Close() = 0;

    virtual bool ParseRecord(const std::string& record, std::string& key, std::string& value) = 0;
    virtual bool BuildRecord(const std::string& key, const std::string& value,
            std::string& record) = 0;
};

}
}

#endif

