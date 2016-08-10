#ifndef _BAIDU_SHUTTLE_MERGEFILE_H_
#define _BAIDU_SHUTTLE_MERGEFILE_H_
#include <vector>

#include "io.h"
#include "sortfile.h"

namespace baidu {
namespace shuttle {

class MergedRecordReader : public RecordReader {
public:
    class MergedRecordIterator : public Iterator {
    public:
        virtual bool Done();
        virtual void Next();
        virtual const std::string& Key();
        virtual const std::string& Value();
        virtual Status Error();
        virtual const std::string GetFileName();
    };
    virtual Status Open(const std::string& path, FileSystem::Parama param);
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual Status Close();
    virtual std::string GetFileName();
private:
    std::vector<SortedRecordReader*> readers_;
    std::string err_file_;
};

}
}

#endif

