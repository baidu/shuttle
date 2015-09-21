#ifndef _BAIDU_SHUTTLE_SORT_FILE_HDFS_H_
#define _BAIDU_SHUTTLE_SORT_FILE_HDFS_H_

#include "sort_file.h"
#include "hdfs.h"

#include <string>

namespace baidu {
namespace shuttle {
class SortFileHdfsReader : public SortFileReader {
public:
    class IteratorHdfs : public Iterator {
    public:
        IteratorHdfs(const std::string& start_key,
                     const std::string& end_key,
                     SortFileHdfsReader* reader);
        virtual ~IteratorHdfs();
        virtual bool Done();
        virtual void Next();
        virtual const std::string& Key();
        virtual const std::string& Value();
        virtual Status Error();
    private:
        SortFileHdfsReader* reader_;
    };

    virtual Status Open(const std::string& path, const Param& param);
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual Status Close();
private:
    hdfsFS fs_;
    hdfsFile fd_;
};

class SortFileHdfsWriter : public SortFileWriter {
public:
    virtual Status Open(const std::string& path, const Param& param);
    virtual Status Put(const std::string& key, const std::string& value);
    virtual Status Close();
};

}
}

#endif

