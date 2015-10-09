#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <gflags/gflags.h>
#include <set>
#include "sort_file.h"
#include "logging.h"

DEFINE_int32(total, 0, "total numbers of map tasks");
DEFINE_int32(reduce_no, 0, "the reduce number of this reduce task");
DEFINE_string(work_dir, "/tmp", "the shuffle work dir");
DEFINE_int32(batch, 100, "merge how many maps output at the same time");

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu::shuttle;

std::set<int32_t> g_merged;

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile("./shuffle_tool.log");
    baidu::common::SetWarningFile("./shuffle_tool.log.wf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    while ((int32_t)g_merged.size() < FLAGS_total) {
        std::vector<std::string> files_to_merge;
        //CollectFilesToMerge(&files_to_merge);
    }
    return 0;
}
