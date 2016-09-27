#include "executor.h"

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <cstdlib>
#include <sys/wait.h>
#include "common/file.h"
#include "common/fileformat.h"
#include "logging.h"
#include "gflags/gflags.h"

DECLARE_int32(max_counter);

namespace baidu {
namespace shuttle {

TaskState Executor::Exec() {
    LOG(INFO, "exec:");
    SetEnv();
    if (!PrepareOutputDir()) {
        LOG(WARNING, "fail to create temp dir for output");
        return kTaskFailed;
    }
    pid_t child_pid = ::fork();
    if (child_pid == -1) {
        LOG(WARNING, "fail to fork a child process for wrapper");
        return kTaskFailed;
    } else if (child_pid == 0) {
        char* cmd_argv[] = { (char*)"sh", (char*)"-c", (char*)"app_wrapper", NULL };
        char* env[] = { NULL };
        ::execve("/bin/sh", cmd_argv, env);
        /*
         * Exec a new program will ignore the rest of the code below,
         *   so it can never get to the following assertion
         */
        assert(0);
    }
    int exit_code = 0;
    ::waitpid(child_pid, &exit_code, 0);
    if (exit_code != 0) {
        LOG(WARNING, "app wrapper return %d, failed", exit_code);
        return kTaskFailed;
    }
    if (!MoveTempDir()) {
        LOG(WARNING, "fail to move temp dir");
        return kTaskMoveOutputFailed;
    }
    return kTaskCompleted;
}

void Executor::SetEnv() {
#define to_str(x) boost::lexical_cast<std::string>(x).c_str()
    ::setenv("mapred_job_id", job_id_.c_str(), 1);
    ::setenv("mapred_job_name", job_.name().c_str(), 1);
    ::setenv("mapred_output_dir", node_.output().path().c_str(), 1);
    ::setenv("mapred_task_partition", to_str(task_.task_id()), 1);
    ::setenv("mapred_attempt_id", to_str(task_.attempt_id()), 1);
    //::setenv("mapred_pre_tasks");
    //::setenv("mapred_next_tasks");
    ::setenv("mapred_memory_limit", to_str(node_.memory() / 1024), 1);
    ::setenv("mapred_task_input", task_.input().input_file().c_str(), 1);
    ::setenv("mapred_task_output", node_.output().path().c_str(), 1);
    ::setenv("map_input_start", to_str(task_.input().input_offset()), 1);
    ::setenv("map_input_length", to_str(task_.input().input_size()), 1);

    //::setenv("minion_identity");
    ::setenv("minion_input_format", node_.input_format() == kBinaryInput ? "seq" : "text", 1);
    ::setenv("minion_input_nline", node_.input_format() == kNLineInput ? "true" : "false", 1);
    ::setenv("minion_phase", to_str(task_.node()), 1);
    ::setenv("minion_pipe_style", job_.pipe_style() == kStreaming ?
            "streaming" : "bistreaming", 1);
    //::setenv("minion_input_dfs_host");
    //::setenv("minion_input_dfs_port");
    //::setenv("minion_input_dfs_user");
    //::setenv("minion_input_dfs_password");
    ::setenv("minion_user_cmd", node_.command().c_str(), 1);
    ::setenv("minion_combiner_cmd", node_.combiner().c_str(), 1);
    ::setenv("minion_partitioner", node_.partition() == kKeyFieldBasedPartitioner ?
            "keyhash" : "inthash", 1);
    ::setenv("minion_key_separator", node_.key_separator().c_str(), 1);
    ::setenv("minion_key_fields", to_str(node_.key_fields_num()), 1);
    ::setenv("minion_partition_fields", to_str(node_.partition_fields_num()), 1);
    ::setenv("minion_output_format", node_.output_format() == kBinaryOutput ?
            "seq" : node_.output_format() == kTextOutput ? "text" : "multiple", 1);
    //::setenv("minion_output_dfs_host");
    //::setenv("minion_output_dfs_port");
    //::setenv("minion_output_dfs_user");
    //::setenv("minion_output_dfs_password");
}

bool Executor::PrepareOutputDir() {
    const std::string temp_dir("_temporary");
    std::string origin = node_.output().path();
    // TODO
    File* fp = NULL;//File::Create();
    if (fp == NULL) {
        LOG(WARNING, "empty file pointer, fail");
        return false;
    }
    if (*origin.rbegin() != '/') {
        origin.push_back('/');
    }
    fp->Mkdir(origin + temp_dir);
    std::stringstream ss;
    ss << origin << temp_dir << "/phase_" << task_.node() << "_"
       << task_.task_id() << "_" << task_.attempt_id();
    if (!fp->Mkdir(ss.str())) {
        LOG(WARNING, "fail to create own directory of phase: %d", task_.node());
        return false;
    }
    return true;
}

bool Executor::ParseCounters(const std::string& work_dir,
        std::map<std::string, int64_t>& counters) {
    std::string err_path = work_dir;
    if (*err_path.rbegin() != '/') {
        err_path.push_back('/');
    }
    err_path += "stderr";
    // Open stderr file as plain text file
    FormattedFile* fp = FormattedFile::Create(kLocalFs, kPlainText, File::Param());
    if (fp == NULL || !fp->Open(err_path, kReadFile, File::Param())) {
        LOG(WARNING, "fail to open user stderr file: %s", err_path.c_str());
        return false;
    }
    std::string line, temp;
    while (fp->ReadRecord(temp, line)) {
        if (static_cast<int32_t>(counters.size()) >= FLAGS_max_counter) {
            break;
        }
        if (!boost::starts_with(line, "reporter:counter:")) {
            continue;
        }
        size_t value_idx = line.find_last_of(',');
        size_t key_idx = line.find_last_of(':');
        if (key_idx == std::string::npos || value_idx == std::string::npos
                || key_idx >= value_idx) {
            continue;
        }
        const std::string& key = line.substr(key_idx + 1, value_idx - (key_idx + 1));
        const std::string& value_str = line.substr(value_idx + 1);
        if (key.empty() || value_str.empty()) {
            continue;
        }
        int64_t value = boost::lexical_cast<int64_t>(value_str);
        counters[key] = value;
    }
    if (fp->Error() != kOk && fp->Error() != kNoMore) {
        LOG(WARNING, "error occurs when reading stderr file: %s",
                Status_Name(fp->Error()).c_str());
        return false;
    }
    fp->Close();
    delete fp;
    return true;
}

bool Executor::MoveTempDir() {
    return false;
}

}
}

