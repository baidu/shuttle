#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <iostream>
#include <gflags/gflags.h>
#include <set>
#include "sort_file.h"
#include "logging.h"
#include "filesystem.h"

DEFINE_int32(total, 0, "total numbers of map tasks");
DEFINE_int32(reduce_no, 0, "the reduce number of this reduce task");
DEFINE_string(work_dir, "/tmp", "the shuffle work dir");
DEFINE_int32(batch, 100, "merge how many maps output at the same time");
DEFINE_int32(attempt_id, 0, "the attempt_id of this reduce task");

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu::shuttle;

std::set<std::string> g_merged;
int32_t g_file_no(0);
FileSystem* g_fs(NULL);

void CollectFilesToMerge(std::vector<std::string>* maps_to_merge) {
    assert(maps_to_merge);
    std::vector<std::string> children;
    bool ok = g_fs->List(FLAGS_work_dir, &children);
    if (!ok) {
        return;
    }
    std::vector<std::string>::iterator it;
    for (it = children.begin(); it != children.end(); it++) {
        const std::string& file_name = *it;
        if (file_name.find("map_") != std::string::npos &&
            g_merged.find(file_name) == g_merged.end()) {
            LOG(INFO, "maps_to_merge: %s", file_name.c_str());
            maps_to_merge->push_back(file_name);
        }
    }
}

void MergeMapOutput(const std::vector<std::string>& maps_to_merge) {
    std::vector<std::string> file_names;
    std::vector<std::string>::const_iterator it;
    std::vector<std::string>::iterator jt;
    std::vector<std::string> real_merged_maps;
    int map_ct = 0;
    for (it = maps_to_merge.begin(); it != maps_to_merge.end(); it++) {
        const std::string& map_dir = *it;
        std::vector<std::string> sort_files;
        if (g_fs->List(map_dir, &sort_files)) {
            for (jt = sort_files.begin(); jt != sort_files.end(); jt++) {
                const std::string& file_name = *jt;
                if (file_name.find(".sort") != std::string::npos) {
                    file_names.push_back(file_name);
                }
            }
        } else {
            LOG(FATAL, "fail to list %s", map_dir.c_str());
        }
        map_ct++;
        if (map_ct >= FLAGS_batch){
            break;
        }
        real_merged_maps.push_back(map_dir);
    }
    if (file_names.empty()) {
        LOG(WARNING, "not map output found");
        return;
    }
    MergeFileReader reader;
    FileSystem::Param param;
    Status status = reader.Open(file_names, param, kHdfsFile);
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
        LOG(FATAL, "fail to close: %s", output_file);
    }
    reader.Close();
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
    std::vector<std::string> children;
    if (!g_fs->List(reduce_merge_dir, &children)) {
        LOG(FATAL, "fail to list: %s", reduce_merge_dir);
    }
    std::vector<std::string> file_names;
    std::vector<std::string>::iterator it;
    for (it = children.begin(); it != children.end(); it++) {
        const std::string& file_name = *it;
        if (file_name.find(".sort") != std::string::npos) {
            file_names.push_back(file_name);
        }
    }
    if (file_names.empty()) {
        LOG(WARNING, "no data for this reduce task");
        return;
    }
    MergeFileReader reader;
    FileSystem::Param param;
    Status status = reader.Open(file_names, param, kHdfsFile);
    if (status != kOk) {
        LOG(FATAL, "fail to open: %s", reader.GetErrorFile().c_str());
    }
    SortFileReader::Iterator* scan_it = reader.Scan("", "");
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(FATAL, "fail to scan: %s", reader.GetErrorFile().c_str());
    }
    while (!scan_it->Done()) {
        std::cout << scan_it->Value() << std::endl;
        scan_it->Next();
    }
    if (scan_it->Error() != kOk && scan_it->Error() != kNoMore) {
        LOG(FATAL, "fail to scan: %s", reader.GetErrorFile().c_str());
    }
    reader.Close();
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile("./shuffle_tool.log");
    baidu::common::SetWarningFile("./shuffle_tool.log.wf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    FileSystem::Param param;
    g_fs = FileSystem::CreateInfHdfs(param);

    while ((int32_t)g_merged.size() < FLAGS_total) {
        std::vector<std::string> maps_to_merge;
        CollectFilesToMerge(&maps_to_merge);
        if (maps_to_merge.size() >= (size_t)FLAGS_batch || 
            maps_to_merge.size() + g_merged.size() >= (size_t)FLAGS_total) {
            MergeMapOutput(maps_to_merge);
        }
        sleep(3);
    }
    MergeAndPrint();
    return 0;
}
