#ifndef _BAIDU_SHUTTLE_MINION_PARTITION_H_
#define _BAIDU_SHUTTLE_MINION_PARTITION_H_
#include <string>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class Partitioner {
public:
    // Factory method
    static Partitioner* Get(Partition partitioner, const NodeConfig& node, int dest_num);

    // Parse line to get key and calc partition result
    virtual int Calc(const std::string& line, std::string* key) const = 0;
    // Get partition result from key itself
    virtual int Calc(const std::string& key) const = 0;

    // String hash function used by paritioner
    int HashCode(const std::string& str) const;

    virtual ~Partitioner() { }
};

} //namespace shuttle
} //namespace baidu

#endif

