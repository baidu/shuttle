#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <unistd.h>
#include <algorithm>
#include <set>
#include <sstream>
#include <iostream>
#include <gflags/gflags.h>
#include <map>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include "sort_file.h"
#include "logging.h"
#include "common/filesystem.h"
#include "common/tools_util.h"
#include "thread_pool.h"
#include "mutex.h"

DEFINE_int32(reduce_no, 0, "the reduce number of this reduce task");
DEFINE_string(work_dir, "/tmp", "the shuffle work dir");
DEFINE_int32(attempt_id, 0, "the attempt_id of this reduce task");
DEFINE_string(dfs_host, "", "host name of dfs master");
DEFINE_string(dfs_port, "", "port of dfs master");
DEFINE_string(dfs_user, "", "user name of dfs master");
DEFINE_string(dfs_password, "", "password of dfs master");
DEFINE_int32(from_no, 0, "from which mapper");
DEFINE_int32(to_no, 0, "to whichi mapper");
DEFINE_int32(tuo_no, 0, "which tuo");

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu;
using namespace baidu::shuttle;

int32_t g_file_no(0);
FileSystem* g_fs(NULL);

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
}

bool AddSortFiles(const std::string map_dir, std::vector<std::string>* file_names) {
    assert(file_names);
    std::vector<FileInfo> sort_files;
    if (g_fs->List(map_dir, &sort_files)) {
        std::vector<FileInfo>::iterator jt;
        for (jt = sort_files.begin(); jt != sort_files.end(); jt++) {
            const std::string& file_name = jt->name;
            if (boost::ends_with(file_name, ".sort")) {
                file_names->push_back(file_name);
            }
        }
        return true;
    } else {
        LOG(WARNING, "fail to list %s", map_dir.c_str());
        return false;
    }
}

bool MergeManyFilesToOne(const std::vector<std::string>& file_names,
                         const std::string& output_file) {
    MergeFileReader reader;
    FileSystem::Param param;
    FillParam(param);
    Status status = reader.Open(file_names, param, kHdfsFile);
    if (status != kOk) {
        LOG(WARNING, "fail to open: %s", reader.GetErrorFile().c_str());
        return false;
    }

    SortFileReader::Iterator* scan_it = reader.Scan("", "");
    boost::scoped_ptr<SortFileReader::Iterator> scan_it_guard(scan_it);

    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(WARNING, "fail to scan: %s", reader.GetErrorFile().c_str());
        return false;
    }
    SortFileWriter* writer = SortFileWriter::Create(kHdfsFile, &status);
    boost::scoped_ptr<SortFileWriter> writer_guard(writer);

    if (status != kOk) {
        LOG(WARNING, "fail to create writer");
        return false;
    }
    FileSystem::Param param_write;
    FillParam(param_write);
    status = writer->Open(output_file, param_write);
    if (status != kOk) {
        LOG(WARNING, "fail to open %s for write", output_file.c_str());
        return false;
    }
    int64_t counter = 0;
    while (!scan_it->Done()) {
        status = writer->Put(scan_it->Key(), scan_it->Value());
        if (status != kOk) {
            LOG(WARNING, "fail to put: %s", output_file.c_str());
            return false;
        }
        counter++;
        if (counter % 5000 == 0) {
            LOG(INFO, "have written %lld records to %s",
                counter, output_file.c_str());
        }
        scan_it->Next();
        if (scan_it->Error() !=kOk && scan_it->Error() != kNoMore) {
            break;
        }
    }
    
    status = writer->Close();
    if (status != kOk) {
        LOG(WARNING, "fail to close writer: %s", output_file.c_str());
        reader.Close();
        return false;
    }
    status = reader.Close();
    if (status != kOk) {
        LOG(WARNING, "fail to close reader: %s", reader.GetErrorFile().c_str());
        return false;
    }
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(WARNING, "fail to scan: %s", reader.GetErrorFile().c_str());
        return false;
    }
    LOG(INFO, "totally written %lld records to %s",
        counter, output_file.c_str());
    return true;
}

bool MergeOneTuo(int map_from, int map_to, int tuo_now) {
    std::vector<std::string> file_names;
    for (int i = map_from; i <= map_to; i++) {
        std::stringstream ss;
        ss << FLAGS_work_dir << "/map_" << i;
        const std::string& map_dir = ss.str();
        if (!AddSortFiles(map_dir, &file_names) || file_names.empty()) {
            return false;
        }
    }
    char tuo_dir[4096];
    snprintf(tuo_dir, sizeof(tuo_dir), "%s/tuo_%d_%d",
             FLAGS_work_dir.c_str(), FLAGS_reduce_no, FLAGS_attempt_id);
    g_fs->Mkdirs(tuo_dir);
    char output_file[4096];
    snprintf(output_file, sizeof(output_file), "%s/tuo_%d_%d/%d.tuo",
            FLAGS_work_dir.c_str(), FLAGS_reduce_no, FLAGS_attempt_id, tuo_now);
    if (!MergeManyFilesToOne(file_names, output_file)) {
        return false;
    }
    std::stringstream ss;
    ss << FLAGS_work_dir << "/" << tuo_now << ".tuo";
    const std::string real_tuo_name = ss.str();
    if (!g_fs->Rename(output_file, real_tuo_name)) {
        g_fs->Remove(output_file);
        return false;
    }
    std::vector<std::string>::iterator it;
    for (it = file_names.begin(); it != file_names.end(); it++) {
        g_fs->Remove(*it);
    }
    return true;
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile(GetLogName("./tuo_merger.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./tuo_merger.log.wf").c_str());
    google::ParseCommandLineFlags(&argc, &argv, true);
    FileSystem::Param param;
    FillParam(param);
    g_fs = FileSystem::CreateInfHdfs(param);
    bool ret = MergeOneTuo(FLAGS_from_no, FLAGS_to_no, FLAGS_tuo_no);
    if (!ret) {
        LOG(WARNING, "tuo_merge fail, [%d, %d] --> tuo(%d)",  FLAGS_from_no, FLAGS_to_no, FLAGS_tuo_no);       
        return 1;
    }
    return 0;
}

