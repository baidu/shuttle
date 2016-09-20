#ifndef _BAIDU_SHUTTLE_INF_HDFS_H_
#define _BAIDU_SHUTTLE_INF_HDFS_H_
#include "common/file.h"
#include "hdfs.h"

namespace baidu {
namespace shuttle {

class InfHdfs : public File {
public:
    InfHdfs() : fs_(NULL), fd_(NULL) { }
    InfHdfs(hdfsFS fs) : fs_(fs), fd_(NULL) { }
    virtual ~InfHdfs() {
        Close();
        if (!fs_) {
            hdfsDisconnect(fs_);
        }
    }

    bool Connect(const Param& param);
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
    virtual bool Mkdir(const std::string& dir);
    virtual bool Exist(const std::string& path);
    virtual std::string GetFileName() {
        return path_;
    }

private:
    hdfsFS fs_;
    hdfsFile fd_;
    std::string path_;
};

}
}

#endif

