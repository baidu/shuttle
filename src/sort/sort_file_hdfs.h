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
        void SetError(Status status);
        void SetHasMore(bool has_more);
    private:
        SortFileHdfsReader* reader_;
        bool has_more_;
        Status error_;
        DataBlock cur_block_;
        int cur_offset_;
        std::string key_;
        std::string value_;
        std::string start_key_;
        std::string end_key_;
    };
    virtual ~SortFileHdfsReader(){};
    virtual Status Open(const std::string& path, Param& param);
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual Status Close();
private:
    Status LoadIndexBlock(const std::string& path);
    Status ReadFull(std::string* result_buf, int32_t len, bool is_read_data = false);
    Status ReadNextRecord(DataBlock& data_block);
private:
    hdfsFS fs_;
    hdfsFile fd_;
    IndexBlock idx_block_;
    std::string path_;
    int64_t idx_offset_;
};

class SortFileHdfsWriter : public SortFileWriter {
public:
    SortFileHdfsWriter();
    virtual ~SortFileHdfsWriter(){};
    virtual Status Open(const std::string& path, Param& param);
    virtual Status Put(const std::string& key, const std::string& value);
    virtual Status Close();
private:
    Status FlushCurBlock();
    Status FlushIdxBlock();
    hdfsFS fs_;
    hdfsFile fd_;
    DataBlock cur_block_;
    IndexBlock idx_block_;
    int32_t cur_block_size_;
    std::string last_key_;
};

}
}

#endif

