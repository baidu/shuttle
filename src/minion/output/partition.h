#ifndef _BAIDU_SHUTTLE_MINION_PARTITION_H_
#define _BAIDU_SHUTTLE_MINION_PARTITION_H_

#include <string>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class Partitioner {
public:
    virtual int Calc(const std::string& line, std::string* key) const = 0;
    virtual int Calc(const std::string& key) const = 0;
    int HashCode(const std::string& str) const;
};

class KeyFieldBasedPartitioner : public Partitioner {
public:
    KeyFieldBasedPartitioner(const TaskInfo& task);
    virtual ~KeyFieldBasedPartitioner(){};
    int Calc(const std::string& line, std::string* key) const;
    int Calc(const std::string& key) const;
private:
    int num_key_fields_;
    int num_partition_fields_;
    int reduce_total_;
    std::string separator_;
};

class IntHashPartitioner : public Partitioner {
public:
    IntHashPartitioner(const TaskInfo& task);
    virtual ~IntHashPartitioner(){};
    int Calc(const std::string& line, std::string* key) const;
    int Calc(const std::string& key) const;
private:
    int reduce_total_;
    std::string separator_;
};

} //namespace shuttle
} //namespace baidu

#endif
