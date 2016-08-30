#ifndef _BAIDU_SHUTTLE_SCANNER_H_
#define _BAIDU_SHUTTLE_SCANNER_H_
#include <string>
#include "fileformat.h"
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

enum ScannerType {
    kInputScanner = 1,
    kInternalScanner = 2
};

/*
 * Scanner provides iterator on formatted files
 *   so operating directly on formatted file is equal to using scanner,
 *   but scanner is more easy to iterate
 */
class Scanner {
public:
    // Scanner requires an opened formatted file
    static Scanner* Get(FormattedFile* fp, ScannerType type);

    class Iterator {
    public:
        virtual bool Done() = 0;
        virtual void Next() = 0;
        virtual Status Error() = 0;
        virtual const std::string& Key() = 0;
        virtual const std::string& Value() = 0;
        virtual std::string GetFileName() = 0;
        virtual ~Iterator() { }
    };
    // For internal file, format must support Locate interface
    virtual Iterator* Scan(const std::string& /*start_key*/,
            const std::string& /*end_key*/) {
        return NULL;
    }
    // For input file, format must support Seek interface
    virtual Iterator* Scan(int64_t /*offset*/, int64_t /*len*/) {
        return NULL;
    }
    virtual std::string GetFileName() = 0;
    virtual ~Scanner() { }
};

}
}

#endif

