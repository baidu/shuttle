#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>

#include <gflags/gflags.h>
#include "input_reader.h"
#include "logging.h"
#include "common/tools_util.h"

using baidu::common::INFO;
using baidu::common::WARNING;
using baidu::common::FATAL;

using namespace baidu::shuttle;

DEFINE_string(file, "", "input file path");
DEFINE_int64(offset, 0, "offset to start read");
DEFINE_int64(len, 1024, "bytes at most read");
DEFINE_string(fs, "hdfs", "file system");
DEFINE_string(dfs_host, "", "host name of dfs master");
DEFINE_string(dfs_port, "", "port of dfs master");
DEFINE_string(dfs_user, "", "user name of dfs master");
DEFINE_string(dfs_password, "", "password of dfs master");
DEFINE_string(format, "text", "input format: text/binary");
DEFINE_string(pipe, "streaming", "pipe style: streaming/bistreaming");

void FillParam(FileSystem::Param& param) {
    if (!FLAGS_dfs_user.empty()) {
        param["host"] = FLAGS_dfs_host;
        param["port"] = FLAGS_dfs_port;
        param["user"] = FLAGS_dfs_user;
        param["password"] = FLAGS_dfs_password;
    }
}

void DoRead() {
    InputReader * reader;
    if (FLAGS_fs == "hdfs") {
        if (FLAGS_format == "text") {
            reader = InputReader::CreateHdfsTextReader();
        } else if (FLAGS_format == "binary") {
            reader = InputReader::CreateSeqFileReader();
        } else {
            LOG(FATAL, "unkown format: %s", FLAGS_format.c_str());
        }
    } else if (FLAGS_fs == "local") {
        reader = InputReader::CreateLocalTextReader();
    } else {
        std::cerr << "unkown file system:" << FLAGS_fs << std::endl;
        exit(-1);
    }
    FileSystem::Param param;
    FillParam(param);
    Status status = reader->Open(FLAGS_file, param);
    if (status != kOk) {
        std::cerr << "fail to open: " << FLAGS_file << std::endl;
        exit(-1);
    }
    InputReader::Iterator* it = reader->Read(FLAGS_offset, FLAGS_len);
    while (!it->Done()) {
        if (FLAGS_pipe == "streaming") {
            std::cout << it->Record() << std::endl;
        } else {
            std::cout << it->Record();// no new line
        }
        it->Next();
    }
    if (it->Error() != kOk && it->Error() != kNoMore) {
        std::cerr << "errors in reading: " << FLAGS_file << std::endl;
        exit(-1);
    }
    delete it;
    reader->Close();
    delete reader;
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile(GetLogName("./input_tool.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./input_tool.log.wf").c_str());
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_file.empty()) {
        std::cerr << "./input_tool -file=[file path] -offset=(offset) -len=(max read)"
                  << std::endl;
        return -1;
    }
    DoRead();
    return 0;
}
