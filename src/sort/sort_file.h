#ifndef _BAIDU_SHUTTLE_SORT_FILE_H_
#define _BAIDU_SHUTTLE_SORT_FILE_H_

#include <algorithm>
#include <map>
#include <string>
#include <queue>
#include <vector>
#include "proto/shuttle.pb.h"
#include "proto/sortfile.pb.h"
#include "common/filesystem.h"
#include "thread_pool.h"
#include "mutex.h"

namespace baidu {
namespace shuttle {

enum FileType {
    kHdfsFile = 0, 
    kNfsFile = 1,
    kLocalFile = 2
};

class SortFileReader {
public:
    static SortFileReader* Create(FileType file_type, Status* status);
    class Iterator {
    public:
        virtual bool Done() = 0;
        virtual void Next() = 0;
        virtual const std::string& Key() = 0;
        virtual const std::string& Value() = 0;
        virtual Status Error() = 0;
        virtual ~Iterator() {};
        virtual const std::string GetFileName() = 0;
    };
    virtual Status Open(const std::string& path, FileSystem::Param param) = 0;
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key) = 0;
    virtual Status Close() = 0;
    virtual std::string GetFileName() = 0;
    virtual ~SortFileReader() {}
};

class SortFileWriter {
public:
    static SortFileWriter* Create(FileType file_type, Status* status);
    virtual Status Open(const std::string& path, FileSystem::Param param) = 0;
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Close() = 0;
    virtual ~SortFileWriter() {}
};

class MergeFileReader {
public:
    struct MergeItem {
        std::string key_;
        std::string value_;
        int it_offset_;
        MergeItem(const std::string& key, const std::string& value, int it_offset) {
            key_ = key;
            value_ = value;
            it_offset_ = it_offset;
        }
        bool operator<(const MergeItem& other) const {
            return key_ > other.key_;
        }
    };

    class MergeIterator : public SortFileReader::Iterator {
    public:
        MergeIterator(const std::vector<SortFileReader::Iterator*>& iters,
                      MergeFileReader* reader);
        virtual ~MergeIterator();
        bool Done();
        void Next();
        const std::string& Key() {return key_;}
        const std::string& Value() {return value_;}
        Status Error() {return status_;};
        const std::string GetFileName() {return "";}
    private:
        std::string key_;
        std::string value_;
        Status status_;
        std::vector<SortFileReader::Iterator*> iters_;
        std::priority_queue<MergeItem> queue_;
        MergeFileReader* merge_reader_;
    };

    ~MergeFileReader();
    Status Open(const std::vector<std::string>& files, 
                FileSystem::Param param,
                FileType file_type);
    SortFileReader::Iterator* Scan(const std::string& start_key, const std::string& end_key);
    Status Close();
    const std::string& GetErrorFile() {return err_file_;}
private:
    SortFileReader::Iterator* AddIter(
                 std::vector<SortFileReader::Iterator*>* iters,
                 SortFileReader* reader,
                 const std::string& start_key,
                 const std::string& end_key);
    void AddReader(const std::string& file_name,
                   FileSystem::Param param,
                   FileType type,
                   Status* st); 
    void CloseReader(SortFileReader* reader, Status* st);
    std::vector<SortFileReader*> readers_;
    std::string err_file_;
    Mutex mu_;
};

}
}
#endif
