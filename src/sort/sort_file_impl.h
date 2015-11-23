#ifndef _BAIDU_SHUTTLE_SORT_FILE_IMPL_H_
#define _BAIDU_SHUTTLE_SORT_FILE_IMPL_H_

#include "sort_file.h"
#include "common/filesystem.h"

namespace baidu {
namespace shuttle {

class SortFileReaderImpl : public SortFileReader {
public:
    class IteratorImpl : public Iterator {
    public:
        IteratorImpl(const std::string& start_key,
                     const std::string& end_key,
                     SortFileReaderImpl* reader);
        virtual ~IteratorImpl();
        virtual bool Done();
        virtual void Next();
        virtual const std::string& Key();
        virtual const std::string& Value();
        virtual Status Error();
        void SetError(Status status);
        void SetHasMore(bool has_more);
        virtual void Init();
        const std::string GetFileName();
    private:
        SortFileReaderImpl* reader_;
        bool has_more_;
        Status error_;
        DataBlock cur_block_;
        int cur_offset_;
        std::string key_;
        std::string value_;
        std::string start_key_;
        std::string end_key_;
    }; //class IteratorImpl

    SortFileReaderImpl(FileSystem* fs) {fs_ = fs;} ;
    virtual ~SortFileReaderImpl(){ delete fs_; };
    virtual Status Open(const std::string& path, FileSystem::Param param);
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual Status Close();
    std::string GetFileName() {return path_;}
private:
    Status LoadIndexBlock(IndexBlock* idx_block);
    Status ReadFull(std::string* result_buf, int32_t len, bool is_read_data = false);
    Status ReadNextRecord(DataBlock& data_block);
private:
    std::string path_;
    int64_t idx_offset_;
    FileSystem* fs_;
};

class SortFileWriterImpl : public SortFileWriter {
public:
    SortFileWriterImpl(FileSystem* fs);
    virtual ~SortFileWriterImpl(){delete fs_; };
    virtual Status Open(const std::string& path, FileSystem::Param param);
    virtual Status Put(const std::string& key, const std::string& value);
    virtual Status Close();
private:
    Status FlushCurBlock();
    Status FlushIdxBlock();
    void MakeIndexSparse();
    DataBlock cur_block_;
    IndexBlock idx_block_;
    int32_t cur_block_size_;
    std::string last_key_;
    FileSystem* fs_;
};

} //namespace shuttle
} //namespace baidu

#endif
