#ifndef _BAIDU_SHUTTLE_FILESYSTEM_H_
#define _BAIDU_SHUTTLE_FILESYSTEM_H_

#include <stdint.h>
#include <string>
#include <map>

namespace baidu {
namespace shuttle {

enum OpenMode {
    kReadFile = 0,
    kWriteFile = 1
};

class FileSystem {
public:
    typedef std::map<std::string, std::string> Param;
    static FileSystem* CreateInfHdfs();
    static FileSystem* CreateInfHdfs(Param param);
    static FileSystem* CreateLocalFs();

    virtual bool Open(const std::string& path,
                      Param param,
                      OpenMode mode) = 0;
    virtual bool Close() = 0;
    virtual bool Seek(int64_t pos) = 0;
    virtual int32_t Read(void* buf, size_t len) = 0;
    virtual int32_t Write(void* buf, size_t len) = 0;
    virtual int64_t Tell() = 0;
    virtual int64_t GetSize() = 0;
    virtual bool Rename(const std::string& old_name, const std::string& new_name) = 0;
    bool WriteAll(void* buf, size_t len);
};

} //namespace shuttle
} //namespace baidu

#endif
