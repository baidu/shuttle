#include "executor.h"

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <cstdlib>
#include <sys/wait.h>
#include <signal.h>
#include "common/file.h"
#include "common/fileformat.h"
#include "logging.h"
#include "gflags/gflags.h"

#define to_str(x) boost::lexical_cast<std::string>(x).c_str()

DECLARE_int32(max_counter);
DECLARE_string(temporary_dir);

namespace baidu {
namespace shuttle {

Executor::Executor(const std::string& job_id, const JobDescriptor& job, const TaskInfo& info)
        : job_id_(job_id), task_(info), job_(job),
          node_(job.nodes(info.node())), wrapper_pid_(-1), scheduler_(job) {
    final_dir_ = node_.output().path();
    if (*final_dir_.rbegin() != '/') {
        final_dir_.push_back('/');
    }
    work_dir_ = final_dir_;
}

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
    wrapper_pid_ = child_pid;
    int exit_code = 0;
    ::waitpid(child_pid, &exit_code, 0);
    if (exit_code != 0) {
        LOG(WARNING, "app wrapper return %d, failed", exit_code);
        return exit_code == 137 ? kTaskKilled : kTaskFailed;
    }
    if (!MoveTempDir()) {
        LOG(WARNING, "fail to move temp dir");
        return kTaskMoveOutputFailed;
    }
    return kTaskCompleted;
}

void Executor::Stop(int32_t task_id) {
    if (task_id != task_.task_id()) {
        return;
    }
    if (wrapper_pid_ == -1) {
        return;
    }
    ::kill(wrapper_pid_, SIGINT);
}

void Executor::SetEnv() {
    ::setenv("mapred_job_id", job_id_.c_str(), 1);
    ::setenv("mapred_job_name", job_.name().c_str(), 1);
    ::setenv("mapred_output_dir", node_.output().path().c_str(), 1);
    ::setenv("mapred_task_partition", to_str(task_.task_id()), 1);
    ::setenv("mapred_attempt_id", to_str(task_.attempt_id()), 1);
    //::setenv("mapred_pre_tasks");
    //::setenv("mapred_next_tasks");
    ::setenv("mapred_memory_limit", to_str(node_.memory() / 1024), 1);
    //::setenv("mapred_task_input", task_.input().input_file().c_str(), 1);
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
    ::setenv("minion_output_dfs_host", node_.output().host().c_str(), 1);
    ::setenv("minion_output_dfs_port", node_.output().port().c_str(), 1);
    ::setenv("minion_output_dfs_user", node_.output().user().c_str(), 1);
    ::setenv("minion_output_dfs_password", node_.output().password().c_str(), 1);
}

bool Executor::PrepareOutputDir() {
    File* fp = File::Create(kInfHdfs, File::BuildParam(node_.output()));
    if (fp == NULL) {
        LOG(WARNING, "empty file pointer, fail");
        return false;
    }
    /*
     * Hierarchy is designed as follows:
     *   given output dir: .../output
     *   * alpha/beta - write internal file:
     *     ../output/_temporary/node_x_x/attempt_x/x.sort
     *     final dir: ../output/_temporary/node_x_x/
     *     work dir: ../output/_temporary/node_x_x/attempt_x/
     *   * omega - write final output file:
     *     ../output/_temporary/part_xxxxx
     *     final dir: ../output/
     *     work dir: ../output/_temporary/
     */
    if (scheduler_.HasSuccessors(task_.task_id())) {
        // For AlphaGru and BetaGru
        // Create temp directory, may have been created
        std::stringstream ss;
        ss << final_dir_ << FLAGS_temporary_dir;
        bool ok = fp->Mkdir(ss.str());
        LOG(DEBUG, "create temp dir %s: %s", ok ? "ok" : "fail", ss.str().c_str());
        // Create own directory of current task, may have been created
        ss << "node_" << task_.node() << "_" << task_.task_id() << "/";
        final_dir_ = ss.str();
        ok = fp->Mkdir(final_dir_);
        LOG(DEBUG, "create phase dir %s: %s", ok ? "ok" : "fail", final_dir_.c_str());
        // Create own directory of current attempt
        ss << "attempt_" << task_.attempt_id() << "/";
        work_dir_ = ss.str();
        if (!fp->Mkdir(work_dir_)) {
            LOG(WARNING, "fail to create own directory of attempt: %d", task_.attempt_id());
            delete fp;
            return false;
        }
    } else {
        // For OmegaGru
        work_dir_ = final_dir_ + FLAGS_temporary_dir;
        bool ok = fp->Mkdir(work_dir_);
        LOG(DEBUG, "create temp dir %s: %s", ok ? "ok" : "fail", work_dir_.c_str());
    }
    delete fp;
    return true;
}

bool Executor::ParseCounters(const std::string& work_dir,
        std::map<std::string, int64_t>& counters) {
    const std::string err_path = work_dir + "stderr";
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
    /*
     * Moving output files follows:
     *   * alpha/beta - move internal file:
     *     ../output/_temporary/node_x_x/attempt_x/x.sort -> ../output/_temporary/node_x_x/x.sort
     *   * omega - move final output file:
     *     ../output/_temporary/part_xxxxx -> ../output/part-xxxxx
     */
    std::string pattern;
    if (scheduler_.HasSuccessors(task_.task_id())) {
        pattern = "*.sort";
    } else {
        pattern = "part_*";
    }
    File* fp = File::Create(kInfHdfs, File::BuildParam(node_.output()));
    if (fp == NULL) {
        LOG(WARNING, "empty file pointer, fail");
        return false;
    }
    std::vector<FileInfo> files;
    if (!fp->Glob(work_dir_ + pattern, &files)) {
        LOG(WARNING, "fail to list work directory: %s", work_dir_.c_str());
        delete fp;
        return false;
    }
    for (std::vector<FileInfo>::iterator it = files.begin();
            it != files.end(); ++it) {
        const std::string& filename = it->name.substr(it->name.find_last_of('/') + 1);
        if (filename.empty() || !File::PatternMatch(filename, pattern)) {
            LOG(WARNING, "try to rename a invalid file: %s", it->name.c_str());
            continue;
        }
        if (!fp->Rename(work_dir_ + filename, final_dir_ + filename)) {
            LOG(WARNING, "fail to move output file to dest: %s", it->name.c_str());
            delete fp;
            return false;
        }
    }
    delete fp;
    return true;
}

}
}

