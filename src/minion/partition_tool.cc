#include <gtest/gtest.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include "partition.h"
using namespace baidu::shuttle;

char g_line_buf[409600];

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("./partition_tool [N reducer]\n");
        return 1;
    }
    TaskInfo task;
    task.mutable_job()->set_reduce_total(atoi(argv[1]));
    KeyFieldBasedPartitioner kf_parti(task);

    while (!feof(stdin)) {
        if (fgets(g_line_buf, 409600, stdin) == NULL) {
            break;
        }
        std::string record(g_line_buf);
        std::string key;
        int reduce_no;
        if (record.size() > 0 && record[record.size() - 1] == '\n') {
            record.erase(record.size() - 1);
        }
        if (record.empty()) {
            continue;
        }
        reduce_no = kf_parti.Calc(record, &key);
        printf("reduce_no: %d, key:%s\n", reduce_no, key.c_str());
    }
    return 0;
}
