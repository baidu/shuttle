#ifndef _BAIDU_SHUTTLE_LOCALFS_H_
#define _BAIDU_SHUTTLE_LOCALFS_H_
#include "common/file.h"

namespace baidu {
namespace shuttle {

class LocalFs : public File {
public:
    LocalFs() : fd_(0) { }
    LocalFs(int fd) : fd_(fd) { }
    virtual ~LocalFs() { }

    virtual bool Open(const std::string& path, OpenMode mode, const Param& param);
    virtual bool Close();
    virtual bool Seek(int64_t pos);
    virtual int32_t Read(void* buf, size_t len);
    virtual int32_t Write(void* buf, size_t len);
    virtual int64_t Tell();
    virtual int64_t GetSize();
    virtual bool Rename(const std::string& old_name, const std::string& new_name);
    virtual bool Remove(const std::string& path);
    virtual bool List(const std::string& dir, std::vector<FileInfo>* children);
    virtual bool Glob(const std::string& /*dir*/, std::vector<FileInfo>* /*children*/) {
        // TODO, not implement, not important for online functions
        return false;
    }
    virtual bool Mkdir(const std::string& dir);
    virtual bool Exist(const std::string& path);
    virtual std::string GetFileName() {
        return path_;
    }
private:
    int fd_;
    std::string path_;
};

}
}

#endif

