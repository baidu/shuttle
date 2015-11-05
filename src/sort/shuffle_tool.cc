#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <gflags/gflags.h>
#include <set>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include "sort_file.h"
#include "logging.h"
#include "common/filesystem.h"
#include "common/tools_util.h"
#include "thread_pool.h"
#include "mutex.h"

DEFINE_int32(total, 0, "total numbers of map tasks");
DEFINE_int32(reduce_no, 0, "the reduce number of this reduce task");
DEFINE_string(work_dir, "/tmp", "the shuffle work dir");
DEFINE_int32(batch, 300, "merge how many maps output at the same time");
DEFINE_int32(attempt_id, 0, "the attempt_id of this reduce task");
DEFINE_string(dfs_host, "", "host name of dfs master");
DEFINE_string(dfs_port, "", "port of dfs master");
DEFINE_string(dfs_user, "", "user name of dfs master");
DEFINE_string(dfs_password, "", "password of dfs master");
DEFINE_string(pipe, "streaming", "pipe style: streaming/bistreaming");
DEFINE_bool(skip_merge, false, "whether skip merge phase");

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu;
using namespace baidu::shuttle;

std::set<std::string> g_merged;
int32_t g_file_no(0);
FileSystem* g_fs(NULL);
Mutex g_mu;

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

void CollectFilesToMerge(std::vector<std::string>* maps_to_merge) {
    assert(maps_to_merge);
    std::vector<std::string> candidates;
    for (int i = 0; i < FLAGS_total; i++) {
        std::stringstream ss;
        ss << FLAGS_work_dir << "/map_" << i;
        std::string map_dir = ss.str();
        if (g_merged.find(map_dir) != g_merged.end()) {
            continue;
        }
        candidates.push_back(map_dir);
    }
    std::vector<std::string>::const_iterator it;
    for (it = candidates.begin(); it != candidates.end(); it++) {
        const std::string& map_dir = *it;
        if (g_fs->Exist(map_dir)) {
            LOG(INFO, "maps_to_merge: %s", map_dir.c_str());
            maps_to_merge->push_back(map_dir);
        }
        if (maps_to_merge->size() >= (size_t)FLAGS_batch) {
            break;
        }
    }
}

void AddSortFiles(const std::string map_dir, std::vector<std::string>* file_names) {
    assert(file_names);
    std::vector<FileInfo> sort_files;
    if (g_fs->List(map_dir, &sort_files)) {
        std::vector<FileInfo>::iterator jt;
        for (jt = sort_files.begin(); jt != sort_files.end(); jt++) {
            const std::string& file_name = jt->name;
            if (boost::ends_with(file_name, ".sort")) {
                MutexLock lock(&g_mu);
                file_names->push_back(file_name);
            }
        }
    } else {
        LOG(FATAL, "fail to list %s", map_dir.c_str());
    }
}

void MergeMapOutput(const std::vector<std::string>& maps_to_merge) {
    std::vector<std::string> * file_names = new std::vector<std::string>();
    boost::scoped_ptr<std::vector<std::string> > file_names_guard(file_names);
    std::vector<std::string>::const_iterator it;
    std::vector<std::string> real_merged_maps;
    ThreadPool pool;
    int map_ct = 0;
    for (it = maps_to_merge.begin(); it != maps_to_merge.end(); it++) {
        const std::string& map_dir = *it;
        pool.AddTask(boost::bind(&AddSortFiles, map_dir, file_names));
        map_ct++;
        real_merged_maps.push_back(map_dir);
        if (map_ct >= FLAGS_batch){
            break;
        }
    }
    LOG(INFO, "wait for list done");
    pool.Stop(true);
    if (file_names->empty()) {
        LOG(WARNING, "not map output found");
        return;
    }
    LOG(INFO, "list #%d *.sort files", file_names->size());
    MergeFileReader reader;
    FileSystem::Param param;
    FillParam(param);
    Status status = reader.Open(*file_names, param, kHdfsFile);
    if (status != kOk) {
        LOG(FATAL, "fail to open: %s", reader.GetErrorFile().c_str());
    }
    char s_reduce_no[256];
    snprintf(s_reduce_no, sizeof(s_reduce_no), "%05d", FLAGS_reduce_no);
    std::string s_reduce_key(s_reduce_no);

    SortFileReader::Iterator* scan_it = reader.Scan(s_reduce_key, 
                                                    s_reduce_key + "\xff");
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(FATAL, "fail to scan: %s", reader.GetErrorFile().c_str());
    }

    char output_file[4096];
    snprintf(output_file, sizeof(output_file), "%s/reduce_%d_%d/%d.sort",
             FLAGS_work_dir.c_str(), FLAGS_reduce_no, FLAGS_attempt_id,
             g_file_no++);
    SortFileWriter * writer = SortFileWriter::Create(kHdfsFile, &status);
    if (status != kOk) {
        LOG(FATAL, "fail to create writer");
    }
    FileSystem::Param param_write;
    FillParam(param_write);
    param_write["replica"] = "2";
    status = writer->Open(output_file, param_write);
    if (status != kOk) {
        LOG(FATAL, "fail to open %s for write", output_file);
    }
    while (!scan_it->Done()) {
        status = writer->Put(scan_it->Key(), scan_it->Value());
        if (status != kOk) {
            LOG(FATAL, "fail to put: %s", output_file);                
        }
        scan_it->Next();
    }
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(FATAL, "fail to scan: %s", reader.GetErrorFile().c_str());
    } 
    status = writer->Close();
    if (status != kOk) {
        LOG(FATAL, "fail to close writer: %s", output_file);
    }
    delete scan_it;
    status = reader.Close();
    if (status != kOk) {
        LOG(FATAL, "fail to close reader: %s", reader.GetErrorFile().c_str());
    }
    delete writer;
    for (it = real_merged_maps.begin(); it != real_merged_maps.end(); it++) {
        LOG(INFO, "g_merged insert: %s", it->c_str());
        g_merged.insert(*it);    
    }
}

void MergeAndPrint() {
    char reduce_merge_dir[4096];
    snprintf(reduce_merge_dir, sizeof(reduce_merge_dir), 
             "%s/reduce_%d_%d",
             FLAGS_work_dir.c_str(), FLAGS_reduce_no, FLAGS_attempt_id);
    std::vector<FileInfo> children;
    if (!g_fs->List(reduce_merge_dir, &children)) {
        LOG(FATAL, "fail to list: %s", reduce_merge_dir);
    }
    std::vector<std::string> file_names;
    std::vector<FileInfo>::iterator it;
    for (it = children.begin(); it != children.end(); it++) {
        const std::string& file_name = it->name;
        if (boost::ends_with(file_name, ".sort")) {
            file_names.push_back(file_name);
        }
    }
    if (file_names.empty()) {
        LOG(WARNING, "no data for this reduce task");
        return;
    }
    MergeFileReader reader;
    FileSystem::Param param;
    FillParam(param);
    Status status = reader.Open(file_names, param, kHdfsFile);
    if (status != kOk) {
        LOG(FATAL, "fail to open: %s", reader.GetErrorFile().c_str());
    }
    SortFileReader::Iterator* scan_it = reader.Scan("", "");
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(FATAL, "fail to scan: %s", reader.GetErrorFile().c_str());
    }
    while (!scan_it->Done()) {
        if (FLAGS_pipe == "streaming") {
            std::cout << scan_it->Value() << std::endl;
        } else {
            std::cout << scan_it->Value();
        }
        scan_it->Next();
    }
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(FATAL, "fail to scan: %s", reader.GetErrorFile().c_str());
    }
    reader.Close();
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile(GetLogName("./shuffle_tool.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./shuffle_tool.log.wf").c_str());
    google::ParseCommandLineFlags(&argc, &argv, true);
    FileSystem::Param param;
    FillParam(param);
    g_fs = FileSystem::CreateInfHdfs(param);
    if (FLAGS_total == 0 ) {
        LOG(FATAL, "invalid map task total");
    }
    while (!FLAGS_skip_merge && (int32_t)g_merged.size() < FLAGS_total) {
        std::vector<std::string> maps_to_merge;
        CollectFilesToMerge(&maps_to_merge);
        if (maps_to_merge.empty()) {
            LOG(INFO, "map output is empty, wait 10 seconds and try...");
            sleep(10);
            continue;
        }
        if (maps_to_merge.size() >= (size_t)FLAGS_batch || 
            maps_to_merge.size() + g_merged.size() >= (size_t)FLAGS_total) {
            LOG(INFO, "try merge %d maps", maps_to_merge.size());
            MergeMapOutput(maps_to_merge);
        } else {
            LOG(INFO, "merged: %d, wait-for merge: %d", g_merged.size(), maps_to_merge.size());
            LOG(INFO, "wait for enough map-output to merge, sleep 5 second");
            sleep(5);
        }
        LOG(INFO, "merge progress: < %d/%d > ", g_merged.size(), FLAGS_total);
    }
    LOG(INFO, "merge and print out");
    MergeAndPrint();
    return 0;
}
