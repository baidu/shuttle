#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include "sort_file.h"
#include "logging.h"
#include "common/tools_util.h"

DEFINE_string(mode, "read", "work mode: read/write/seek");
DEFINE_string(file, "", "file path, use ',' to separate multiple files");
DEFINE_string(start, "", "start key, in 'read' mode");
DEFINE_string(end, "", "end key, in 'read' mode");
DEFINE_string(fs, "hdfs", "filesytem: 'hdfs' or 'local' ");
DEFINE_string(replica, "3", "the replication number on dfs");

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu::shuttle;

char g_line_buf[4096000];
FileType g_file_type;

void DoRead() {
    std::vector<std::string> file_names;
    boost::split(file_names, FLAGS_file,
                 boost::is_any_of(","), boost::token_compress_on);
    if (file_names.size() == 0 || FLAGS_file.empty()) {
        std::cerr << "use -file to specify input files" << std::endl;
        exit(-1);
    }
    MergeFileReader* reader = new MergeFileReader();
    FileSystem::Param param;
    if (getenv("minion_input_dfs_host") != NULL) {
        param["host"] = getenv("minion_input_dfs_host");
    }
    if (getenv("minion_input_dfs_port") != NULL) {
        param["port"] = getenv("minion_input_dfs_port");
    }
    if (getenv("minion_input_dfs_user") != NULL) {
        param["user"] = getenv("minion_input_dfs_user");
    }
    if (getenv("minion_input_dfs_password") != NULL) {
        param["password"] = getenv("minion_input_dfs_password");
    }

    Status status = reader->Open(file_names, param, g_file_type);
    if (status != kOk) {
        std::cerr << "fail to open: " << reader->GetErrorFile() << std::endl;
        exit(-1);
    }
    SortFileReader::Iterator* it = reader->Scan(FLAGS_start, FLAGS_end);
    if (it->Error() != kOk && it->Error() != kNoMore) {
        std::cerr << "fail top scan: " << reader->GetErrorFile() 
                  << ", " << Status_Name(it->Error()) << std::endl;
        exit(-1);
    }
    while (!it->Done()) {
        if (it->Error() != kOk && it->Error() != kNoMore) {
            std::cerr << "error happen in reading: " 
                      << reader->GetErrorFile()
                      << Status_Name(it->Error()) << std::endl;
            exit(-2);
        }
        std::cout << it->Key() << "\t" << it->Value() << std::endl;
        it->Next();
    }
    if (it->Error() != kOk && it->Error() != kNoMore) {
        std::cerr << "error happen in reading: " 
                  << reader->GetErrorFile()
                  << Status_Name(it->Error()) << std::endl;
        exit(-2);
    }
    delete it;
    status = reader->Close();
    if (status != kOk) {
        std::cerr << "fail to close: " 
                  << reader->GetErrorFile()
                  << Status_Name(status) << std::endl;
        exit(-1);
    }
    std::cerr << "== Read Done ==" << std::endl;
    return;
}

void DoWrite() {
    if (FLAGS_file.empty()) {
        std::cerr << "use -file to specify output file" << std::endl;
        exit(-1);
    }
    Status status;
    SortFileWriter * writer = SortFileWriter::Create(g_file_type, &status);
    if (status != kOk) {
        std::cerr << "fail to create writer" << std::endl;
        exit(-1);
    }
    FileSystem::Param param;
    param["replica"] = std::string(FLAGS_replica);
    if (getenv("minion_output_dfs_host") != NULL) {
        param["host"] = getenv("minion_output_dfs_host");
    }
    if (getenv("minion_output_dfs_port") != NULL) {
        param["port"] = getenv("minion_output_dfs_port");
    }
    if (getenv("minion_output_dfs_user") != NULL) {
        param["user"] = getenv("minion_output_dfs_user");
    }
    if (getenv("minion_output_dfs_password") != NULL) {
        param["password"] = getenv("minion_output_dfs_password");
    }
    status = writer->Open(FLAGS_file, param);
    if (status != kOk) {
        std::cerr << "fail to open for write:" << FLAGS_file << std::endl;
        exit(-1);
    }
    std::cerr << "Enter: key [tab] value per line" << std::endl;
    int count = 0;
    while (!feof(stdin)) {
        if (fgets(g_line_buf, sizeof(g_line_buf), stdin) ==  NULL) {
            break;
        }
        int span = strcspn(g_line_buf, "\t");
        std::string line(g_line_buf);
        if (line.size() > 0 && line[line.size()-1] == '\n') {
            line.erase(line.size() - 1);
        }
        std::string key = line.substr(0, span);
        std::string value;
        if (span + 1 < (int)line.size()) {
            value = line.substr(span + 1);
        }
        status = writer->Put(key, value);
        if (status != kOk) {
            std::cerr << "fail to put: " 
                      << key << " --> " << value
                      << ", Status:" << Status_Name(status)
                      << std::endl;
            exit(-1);
        }
        if (count % 10000 == 0) {
            std::cerr << "have written " << count << " records" << std::endl;
        }
        count++;
    }
    status = writer->Close();
    if (status != kOk) {
        std::cerr << "fail to close: " << FLAGS_file << std::endl;
        exit(-1);
    }
    std::cerr << "have written " << count << " records" << std::endl;
    std::cerr << "== Write Done ==" << std::endl;
}

void DoSeek() {
    std::vector<std::string> file_names;
    boost::split(file_names, FLAGS_file,
                 boost::is_any_of(","), boost::token_compress_on);
    if (file_names.size() == 0 || FLAGS_file.empty()) {
        std::cerr << "use -file to specify input files" << std::endl;
        exit(-1);
    }
    MergeFileReader* reader = new MergeFileReader();
    Status status;
    FileSystem::Param param;
    if (getenv("minion_input_dfs_host") != NULL) {
        param["host"] = getenv("minion_input_dfs_host");
    }
    if (getenv("minion_input_dfs_port") != NULL) {
        param["port"] = getenv("minion_input_dfs_port");
    }
    if (getenv("minion_input_dfs_user") != NULL) {
        param["user"] = getenv("minion_input_dfs_user");
    }
    if (getenv("minion_input_dfs_password") != NULL) {
        param["password"] = getenv("minion_input_dfs_password");
    }

    status = reader->Open(file_names, param, g_file_type);
    if (status != kOk) {
        std::cerr << "fail to open for read:" << FLAGS_file << std::endl;
        exit(-1);
    }
    std::cerr << "Enter: key per line" << std::endl;
    while (!feof(stdin)) {
        if (fgets(g_line_buf, sizeof(g_line_buf), stdin) ==  NULL) {
            break;
        }
        int span = strcspn(g_line_buf, "\t");
        std::string line(g_line_buf);
        if (line.size() > 0 && line[line.size()-1] == '\n') {
            line.erase(line.size() - 1);
        }
        std::string key = line.substr(0, span);
        SortFileReader::Iterator* it = reader->Scan(key, key + "\1");
        if (it->Error() != kOk && it->Error() != kNoMore) {
            std::cerr << "fail top scan: " << FLAGS_file 
                      << ", " << Status_Name(it->Error()) << std::endl;
            exit(-1);
        }
        int ct = 0;
        while (!it->Done()) {
            if (it->Error() != kOk && it->Error() != kNoMore) {
                std::cerr << "error ocurrs, status: " 
                          << Status_Name(it->Error())
                          << std::endl;
            } else {
                std::cout << key << "\t" << it->Value() << std::endl;
                ct++;
            }
            it->Next();
        }
        if (ct == 0 ) {
            std::cerr << key << "\tNOT_FOUND" << std::endl; 
        } 
        delete it;
    }
    status = reader->Close();
    if (status != kOk) {
        std::cerr << "fail to close: " << FLAGS_file << std::endl;
        exit(-1);
    }
    std::cerr << "== Seek Done ==" << std::endl;
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile("./sf_tool.log");
    baidu::common::SetWarningFile("./sf_tool.log.wf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_fs == "hdfs") {
        g_file_type = kHdfsFile;
    } else if (FLAGS_fs == "local") {
        g_file_type = kLocalFile;
    } else {
        std::cerr << "unkonw file type: " << FLAGS_fs << std::endl;
        return -1;
    }
    if (FLAGS_mode == "read") {
        DoRead();
    } else if (FLAGS_mode == "write") {
        DoWrite();
    } else if (FLAGS_mode == "seek") {
        DoSeek();
    } else {
        std::cerr << "unkown work mode:" << FLAGS_mode << std::endl;
        return 1;
    }
    return 0;
}
