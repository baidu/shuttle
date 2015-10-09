#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <gflags/gflags.h>
#include "sort_file.h"
#include "logging.h"

DEFINE_int32(total, 0, "total numbers of map tasks");
DEFINE_int32(reduce_no, 0, "the reduce number of this reduce task");
DEFINE_string(work_dir, "/tmp", "the shuffle work dir");

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu::shuttle;

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile("./shuffle_tool.log");
    baidu::common::SetWarningFile("./shuffle_tool.log.wf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    return 0;
}
