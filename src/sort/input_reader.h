#ifndef _BAIDU_SHUTTLE_SORT_INPUT_READER_
#define _BAIDU_SHUTTLE_SORT_INPUT_READER_
#include <string>
#include "common/filesystem.h"
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class InputReader {
public:
    static InputReader* CreateHdfsTextReader();
    static InputReader* CreateLocalTextReader();
    static InputReader* CreateSeqFileReader();

    class Iterator {
    public:
        virtual bool Done() = 0;
        virtual void Next() = 0;
        virtual const std::string& Record() = 0;
        virtual Status Error() = 0;
        virtual ~Iterator() { }
    };
    virtual Status Open(const std::string& path, FileSystem::Param param) = 0;
    virtual Iterator* Read(int64_t offset, int64_t len) = 0;
    virtual Status Close() = 0;
    virtual ~InputReader() {}
};

}
}

#endif
