#ifndef _BAIDU_SHUTTLE_SORTFILE_H_
#define _BAIDU_SHUTTLE_SORTFILE_H_
#include "io.h"
#include "common/filesystem.h"

namespace baidu {
namespace shuttle {

class SortedRecordReader : public RecordReader {
public:
    class SortedRecordIterator : public Iterator {
    public:
        // Iterator Interfaces
        virtual ~SortedRecordIterator();
        virtual bool Done();
        virtual void Next();
        virtual const std::string& Key();
        virtual const std::string& Value();
        virtual Status Error();
        virtual const std::string GetFileName();

        // Functions for the access of outer reader
        void SetError(Status status);
        void SetHasMore(bool has_more);
    private:
        SortedRecordReader reader_;
        bool has_more_;
        Status error_;
        int64_t cur_offset_;
        std::string key_;
        std::string value_;
        std::string start_key_;
        std::string end_key_;
    };

    SortedRecordReader(FileSystem* fs) : fs_(fs) { }
    virtual ~SortedRecordReader() { delete fs_; }
    // Inherited from base RecordReader
    virtual Status Open(const std::string& path, FileSystem::Param& param);
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual Status Close();
    virtual std::string GetFileName();
private:
    std::string path_;
    int64_t idx_offset_;
    FileSystem* fs_;
};

class SortedRecordWriter : public RecordWriter {
public:
    virtual Status Open(const std::string& path, FileSystem::Param param);
    virtual Status Put(const std::string& key, const std::string& value);
    virtual Status Close();
};

}
}

#endif

