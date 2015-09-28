#include "partition.h"
#include <boost/algorithm/string.hpp>

namespace baidu {
namespace shuttle {

KeyFieldBasedPartitioner::KeyFieldBasedPartitioner(const TaskInfo& task) 
  : num_key_fields_(0),
    num_partition_fields_(0), 
    reduce_total_(0) {
    num_key_fields_ = task.job().key_fields_num();
    num_partition_fields_ = task.job().partition_fields_num();
    reduce_total_ = task.job().reduce_total();
    separator_ = task.job().key_separator();
    if (num_key_fields_ == 0) {
        num_key_fields_ = 1;
    }
    if (num_partition_fields_ == 0) {
        num_partition_fields_ = 1;
    }
}

int KeyFieldBasedPartitioner::Calc(const std::string& line, std::string* key) {
    std::vector<std::string> fields;
    boost::split(fields, line, boost::is_any_of(separator_));
    //TODO
    return 0;    
}

IntHashPartitioner::IntHashPartitioner(const TaskInfo& task)
  : reduce_total_(0) {
    reduce_total_ = task.job().reduce_total();
}

int IntHashPartitioner::Calc(const std::string& line, std::string* key) {
    //TODO
    return 0;
}


} //namespace shuttle
} //namespace baidu
