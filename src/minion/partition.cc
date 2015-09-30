#include "partition.h"
#include <algorithm>

namespace baidu {
namespace shuttle {

int Partitioner::HashCode(const std::string& str) {
    int h = 1;
    if (str.empty()) {
        return 0;
    }
    for (size_t i = 0;  i < str.size(); i++) {
        h = 31 * h + str[i];
    }
    return h & 0x7FFFFFFF;
}

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
    if (separator_.empty()) {
        separator_ = "\t";
    }
}

int KeyFieldBasedPartitioner::Calc(const std::string& line, std::string* key) {
    assert(key);
    const char* head = line.data();
    const char *p1 = head;
    const char *p2 = head;
    const char* end = head + line.size();
    int N = std::max(num_key_fields_, num_partition_fields_);
    for (int i = 0; i < N; i++) {
        if (i < num_key_fields_) {
            if (p1 >= end) {
                break;
            }
            p1 += (strcspn(p1, separator_.c_str()) + 1);
        }
        if (i < num_partition_fields_) {
            if (p2 >= end) {
                break;
            }
            p2 += (strcspn(p2, separator_.c_str()) + 1);
        }
    }
    if (p1 == head) {
        p1 = head + 1;
    }
    if (p2 == head) {
        p2 = head + 1;
    }
    key->assign(head, p1 - 1);
    std::string partition_key(head, p2 - 1);
    //printf("zzzzzzzzzzzzz: %s\n", partition_key.c_str());
    return HashCode(partition_key) % reduce_total_; 
}

IntHashPartitioner::IntHashPartitioner(const TaskInfo& task)
  : reduce_total_(0) {
    reduce_total_ = task.job().reduce_total();
    separator_ = task.job().key_separator();
    if (separator_.empty()) {
        separator_ = "\t";
    }
}

int IntHashPartitioner::Calc(const std::string& line, std::string* key) {
    size_t space_pos = line.find(" "); //e.g "123 key_xxx\tvalue"
    int hash_code;
    if (space_pos != std::string::npos) {
        hash_code = atoi(line.substr(0, space_pos).c_str());
        const char *p = line.data() + space_pos + 1;
        int key_span = strcspn(p, separator_.c_str());
        key->assign(p, key_span);
    } else { // no white space found
        const char *p = line.data();
        int key_span = strcspn(p, separator_.c_str());
        key->assign(p, key_span);
        hash_code = HashCode(*key);
    }
    return hash_code % reduce_total_;
}

} //namespace shuttle
} //namespace baidu
