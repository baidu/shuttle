#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>

#include <gflags/gflags.h>
#include "input_reader.h"
#include "logging.h"

using baidu::common::INFO;
using baidu::common::WARNING;

using namespace baidu::shuttle;

DEFINE_string(file, "", "input file path");
DEFINE_int64(offset, 0, "offset to start read");
DEFINE_int64(len, 1024, "bytes at most read");
DEFINE_string(fs, "hdfs", "file system");

void DoRead() {
    InputReader * reader;
    if (FLAGS_fs == "hdfs") {
        reader = InputReader::CreateHdfsTextReader();
    } else if (FLAGS_fs == "local") {
        reader = InputReader::CreateLocalTextReader();
    } else {
        std::cerr << "unkown file system:" << FLAGS_fs << std::endl;
        exit(-1);
    }
    FileSystem::Param param;
    Status status = reader->Open(FLAGS_file, param);
    if (status != kOk) {
        std::cerr << "fail to open: " << FLAGS_file << std::endl;
        exit(-1);
    }
    InputReader::Iterator* it = reader->Read(FLAGS_offset, FLAGS_len);
    while (!it->Done()) {
        std::cout << it->Line() << std::endl;
        it->Next();
    }
    if (it->Error() != kOk && it->Error() != kNoMore) {
        std::cerr << "errors in reading: " << FLAGS_file << std::endl;
    }
    delete it;
    reader->Close();
    delete reader;
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile("./input_tool.log");
    baidu::common::SetWarningFile("./input_tool.log.wf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_file.empty()) {
        std::cerr << "./input_tool -file=[file path] -offset=(offset) -len=(max read)"
                  << std::endl;
        return -1;
    }
    DoRead();
    return 0;
}
