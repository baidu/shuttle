#ifndef _BAIDU_SHUTTLE_FILE_H_
#define _BAIDU_SHUTTLE_FILE_H_

#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

enum FileType {
    kLocalFs = 1,
    kInfHdfs = 2
};

enum OpenMode {
    kReadFile = 0,
    kWriteFile = 1
};

struct FileInfo {
    char kind;
    std::string name;
    int64_t size;
};

class File {
public:
    typedef std::map<std::string, std::string> Param;
    static File* Create(FileType type, const Param& param);

    virtual bool Open(const std::string& path, OpenMode mode) = 0;
    virtual bool Close() = 0;
    virtual bool Seek(int64_t pos) = 0;
    virtual int32_t Read(void* buf, size_t len) = 0;
    virtual int32_t Write(void* buf, size_t len) = 0;
    virtual int64_t Tell() = 0;
    virtual int64_t GetSize() = 0;
    virtual bool Rename(const std::string& old_name, const std::string& new_name) = 0;
    virtual bool Remove(const std::string& path) = 0;
    bool WriteAll(void* buf, size_t len);
    virtual bool List(const std::string& dir, std::vector<FileInfo>* children) = 0;
    virtual bool Glob(const std::string& dir, std::vector<FileInfo>* children) = 0;
    virtual bool Mkdirs(const std::string& dir) = 0;
    virtual bool Exist(const std::string& path) = 0;
};

class FileHub {
public:
    virtual File* BuildFs(DfsInfo& info) = 0;
    virtual File* GetFs(const std::string& address) = 0;
    virtual File::Param GetParam(const std::string& address) = 0;

    static FileHub* GetHub();
    static File::Param BuildFileParam(DfsInfo& info);
};

} //namespace shuttle
} //namespace baidu

#endif

