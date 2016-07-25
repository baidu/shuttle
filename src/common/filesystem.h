#ifndef _BAIDU_SHUTTLE_FILESYSTEM_H_
#define _BAIDU_SHUTTLE_FILESYSTEM_H_

#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include "proto/shuttle.pb.h"
#include "hdfs.h" //for hdfs of inf

namespace baidu {
namespace shuttle {

enum OpenMode {
    kReadFile = 0,
    kWriteFile = 1
};

struct FileInfo {
    char kind;
    std::string name;
    int64_t size;
    FileInfo() { }
    FileInfo(const hdfsFileInfo& hdfsfile) :
            kind(hdfsfile.mKind),
            name(hdfsfile.mName),
            size(hdfsfile.mSize) {
    }
};

class FileSystem {
public:
    typedef std::map<std::string, std::string> Param;
    static FileSystem* CreateInfHdfs();
    static FileSystem* CreateInfHdfs(Param& param);
    static FileSystem* CreateLocalFs();

    virtual bool Open(const std::string& path,
                      OpenMode mode) = 0;
    virtual bool Open(const std::string& path,
                      Param& param,
                      OpenMode mode) = 0;
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

class FileSystemHub {
public:
    virtual FileSystem* BuildFs(DfsInfo& info) = 0;
    virtual FileSystem* GetFs(const std::string& address) = 0;
    virtual FileSystem::Param GetParam(const std::string& address) = 0;

    static FileSystemHub* GetHub();
    static FileSystem::Param BuildFileParam(DfsInfo& info);
};

class InfSeqFile {
public:
    InfSeqFile();
    bool Open(const std::string& path, FileSystem::Param& param, OpenMode mode);
    bool Close();
    bool ReadNextRecord(std::string* key, std::string* value, bool* eof);
    bool WriteNextRecord(const std::string& key, const std::string& value);
    bool Seek(int64_t offset);
    int64_t Tell();
private:
    hdfsFS fs_;
    SeqFile sf_;
    std::string path_;
};

} //namespace shuttle
} //namespace baidu

#endif
