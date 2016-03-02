#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <limits>
#include <boost/lexical_cast.hpp>

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
DEFINE_bool(is_nline, false, "whether NlineInputformat");
DEFINE_bool(decompress_input, false, "whether decompreess input file");

void FillParam(FileSystem::Param& param) {
    if (!FLAGS_dfs_user.empty()) {
        param["user"] = FLAGS_dfs_user;
    }
    if (!FLAGS_dfs_password.empty()) {
        param["password"] = FLAGS_dfs_password;
    }
    if (!FLAGS_dfs_host.empty()) {
        param["host"] = FLAGS_dfs_host;
    }
    if (!FLAGS_dfs_port.empty()) {
        param["port"] = FLAGS_dfs_port;
    }
    if (FLAGS_decompress_input) {
        param["decompress"] = "true";
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
    if (FLAGS_decompress_input) {
        FLAGS_offset = 0;
        FLAGS_len = std::numeric_limits<int64_t>::max();
    }
    InputReader::Iterator* it = reader->Read(FLAGS_offset, FLAGS_len);
    int32_t record_no = 0;
    bool should_print_eol = false;
    if (FLAGS_is_nline) {
        should_print_eol = true;
    }
    if (FLAGS_pipe == "streaming") {
        if (FLAGS_format == "text") {
            should_print_eol = true;
        } else if (FLAGS_format == "binary") {
            should_print_eol = false;
        }
    }
    while (!it->Done()) {
        if (should_print_eol) {
            if (FLAGS_is_nline) {
                std::cout << record_no << "\t" << it->Record() << std::endl;
            } else {
                std::cout << it->Record() << std::endl;
            }
        } else {
            std::cout << it->Record();// no new line
            //std::cerr << record_no << std::endl;
        }
        it->Next();
        record_no ++;
    }
    if (it->Error() != kOk && it->Error() != kNoMore) {
        std::cerr << "errors in reading: " << FLAGS_file << std::endl;
        exit(-1);
    }
    delete it;
    reader->Close();
    delete reader;
    std::cerr << "totoal records:" << record_no << std::endl;
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
    std::string host;
    int port;
    ParseHdfsAddress(FLAGS_file, &host, &port, NULL);
    if (!host.empty() && host != FLAGS_dfs_host) { // when conflict
        FLAGS_dfs_host = host;
        FLAGS_dfs_port = boost::lexical_cast<std::string>(port);
        FLAGS_dfs_user = "";
        FLAGS_dfs_password = "";
    }
    DoRead();
    return 0;
}
