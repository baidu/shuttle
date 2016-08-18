#ifndef _BAIDU_SHUTTLE_IO_TYPE_H_
#define _BAIDU_SHUTTLE_IO_TYPE_H_
#include <string>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class IteratorPrototype {
    virtual bool Done() = 0;
    virtual void Next() = 0;
    virtual Status Error() = 0;

    virtual ~IteratorPrototype() { }
};

class KVScanner {
public:
    class Iterator : public IteratorPrototype {
    public:
        // virtual bool Done() = 0;
        // virtual void Next() = 0;
        // virtual Status Error() = 0;
        virtual const std::string& Key() = 0;
        virtual const std::string& Value() = 0;
        virtual const std::string GetFileName() = 0;
        virtual ~Iterator() { }
    };
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key) = 0;

    virtual Status Open(const std::string& path, const File::Param& param) = 0;
    virtual Status Close() = 0;
    virtual std::string GetFileName() = 0;
    virtual ~FileReader() { }
};

class KVWriter {
public:
    virtual Status Put(const std::string& key, const std::string& value) = 0;

    virtual Status Open(const std::string& path, const File::Param& param) = 0;
    virtual Status Close() = 0;
    virtual ~FileWriter() { }
};

class RecordReader {
public:
    class Iterator : public IteratorPrototype {
    public:
        // virtual bool Done() = 0;
        // virtual void Next() = 0;
        // virtual Status Error() = 0;
        virtual const std::string& Record() = 0;
        virtual ~Iterator() { }
    };
    virtual Iterator* Read(int64_t offset, int64_t len) = 0;

    // virtual Status Open(const std::string& path, const File::Param& param) = 0;
    // virtual Status Close() = 0;
    virtual ~InputReader() { }
};

}
}

#endif

