#ifndef _BAIDU_SHUTTLE_SORT_FILE_H_
#define _BAIDU_SHUTTLE_SORT_FILE_H_

#include <map>
#include <string>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class SortFileReader {
public:
    class Iterator {
    public:
        virtual bool Done() = 0;
        virtual void Next() = 0;
        virtual const std::string& Key() = 0;
        virtual const std::string& Value() = 0;
        virtual Status Error() = 0;
        virtual ~Iterator() {};
    };
    typedef std::map<std::string, std::string> Param;
    virtual Status Open(const std::string& path, Param& param) = 0;
    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key) = 0;
    virtual Status Close() = 0;
};

class SortFileWriter {
public:
    typedef std::map<std::string, std::string> Param;
    virtual Status Open(const std::string& path, Param& param) = 0;
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Close() = 0;
};

}
}
#endif
