#include "executor.h"
#include <unistd.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>

namespace baidu {
namespace shuttle {

Executor::Executor() {
    line_buf_ = (char*)malloc(sLineBufferSize);
}

Executor::~Executor() {
    free(line_buf_);
}

void Executor::Stop(int32_t task_id) {
    MutexLock locker(&mu_);
    stop_task_ids_.insert(task_id);
}

bool Executor::ShouldStop(int32_t task_id) {
    MutexLock locker(&mu_);
    if (stop_task_ids_.find(task_id) != stop_task_ids_.end()) {
        return true;
    }
    return false;
}

Executor* Executor::GetExecutor(WorkMode mode) {
    Executor* executor;
    switch(mode) {
    case kMap:
        executor = new MapExecutor();
        break;
    case kReduce:
        executor = new ReduceExecutor();
        break;
    case kMapOnly:
        executor = new MapOnlyExecutor();
        break;
    default:
        LOG(FATAL, "unkonw work mode: %d", mode);
        abort();
        break;
    }
    return executor;
}

void Executor::SetEnv(const std::string& jobid, const TaskInfo& task,
                      WorkMode mode) {
    {
        MutexLock locker(&mu_);
        stop_task_ids_.clear();
    }
    ::setenv("mapred_job_id", jobid.c_str(), 1);
    ::setenv("mapred_job_name", task.job().name().c_str(), 1);
    ::setenv("mapred_output_dir", task.job().output().c_str(), 1);
    ::setenv("map_input_file", task.input().input_file().c_str(), 1);
    ::setenv("map_input_start", 
             boost::lexical_cast<std::string>(task.input().input_offset()).c_str(), 
             1);
    ::setenv("map_input_length", 
             boost::lexical_cast<std::string>(task.input().input_size()).c_str(),
             1);

    ::setenv("mapred_map_tasks", 
             boost::lexical_cast<std::string>(task.job().map_total()).c_str(), 
            1);
    ::setenv("mapred_reduce_tasks", 
             boost::lexical_cast<std::string>(task.job().reduce_total()).c_str(), 
            1);
    ::setenv("mapred_task_partition", 
             boost::lexical_cast<std::string>(task.task_id()).c_str(), 
            1);
    ::setenv("mapred_memory_limit",
             boost::lexical_cast<std::string>(task.job().memory() / 1024).c_str(),
             1);
    std::stringstream ss;
    ss << "attempt_" << jobid << "_" << task.task_id() << "_" << task.attempt_id();
    ::setenv("mapred_task_id",
             ss.str().c_str(), 
            1);

    ::setenv("mapred_attempt_id",
             boost::lexical_cast<std::string>(task.attempt_id()).c_str(),
             1);
    ::setenv("minion_shuffle_work_dir", GetShuffleWorkDir(task).c_str(), 1);
    ::setenv("minion_input_dfs_host", task.job().input_dfs().host().c_str(), 1);
    ::setenv("minion_input_dfs_port", task.job().input_dfs().port().c_str(), 1);
    ::setenv("minion_input_dfs_user", task.job().input_dfs().user().c_str(), 1);
    ::setenv("minion_input_dfs_password", task.job().input_dfs().password().c_str(), 1);
    ::setenv("minion_output_dfs_host", task.job().output_dfs().host().c_str(), 1);
    ::setenv("minion_output_dfs_port", task.job().output_dfs().port().c_str(), 1);
    ::setenv("minion_output_dfs_user", task.job().output_dfs().user().c_str(), 1);
    ::setenv("minion_output_dfs_password", task.job().output_dfs().password().c_str(), 1);

    bool is_map = false;
    char* c_is_map = getenv("mapred_task_is_map");
    if (c_is_map != NULL) {
        std::string s_is_map = c_is_map;
        if (s_is_map == "true") {
            is_map = true;
        }
    }
    if (!task.job().combine_command().empty() && is_map) {
        std::string combiner_cmd = "./combine_tool -cmd '" 
                                   + task.job().combine_command() + "' ";
        if (task.job().partition() == kIntHashPartitioner) {
            combiner_cmd += "-is_inthash=true ";
        }
        if (task.job().key_fields_num() != 1) {
            combiner_cmd += ("-num_key_fields=" + boost::lexical_cast<std::string>(task.job().key_fields_num()) + " ");
        }
        if (task.job().pipe_style() == kStreaming) {
            combiner_cmd += "-pipe streaming ";
        } else if (task.job().pipe_style() == kBiStreaming) {
            combiner_cmd += "-pipe bistreaming ";
        }
        if (!task.job().key_separator().empty()) {
            combiner_cmd += "-separator '" + task.job().key_separator() +"' ";
        }
        ::setenv("minion_combiner_cmd", combiner_cmd.c_str(), 1);
        LOG(INFO, "combiner_cmd: %s", combiner_cmd.c_str());
    }
    if (task.job().input_format() == kTextInput) {
        ::setenv("minion_input_format", "text", 1);
        if (task.job().has_decompress_input() 
            && task.job().decompress_input()) {
            ::setenv("minion_decompress_input", "true", 1);
        }
    } else if (task.job().input_format() == kBinaryInput) {
        ::setenv("minion_input_format", "binary", 1);
    } else if (task.job().input_format() == kNLineInput) {
        ::setenv("minion_input_format", "text", 1);
        ::setenv("minion_input_is_nline", "true", 1);
    }
    if (task.job().output_format() == kTextOutput) {
        ::setenv("minion_output_format", "text", 1);
        if (task.job().has_compress_output()
            && task.job().compress_output()
            && (mode == kReduce || mode == kMapOnly)) {
            ::setenv("minion_compress_output", "true", 1);
        }
    } else if (task.job().output_format() == kBinaryOutput) {
        ::setenv("minion_output_format", "binary", 1);
    }
    if (task.job().pipe_style() == kStreaming) {
        ::setenv("minion_pipe_style", "streaming", 1);
    } else if (task.job().pipe_style() == kBiStreaming) {
        ::setenv("minion_pipe_style", "bistreaming", 1);
    }
}

const std::string Executor::GetShuffleWorkDir(const TaskInfo& task) {
    std::string shuffle_work_dir = task.job().output() + "/_temporary/shuffle";
    return shuffle_work_dir;
}

const std::string Executor::GetMapWorkFilename(const TaskInfo& task) {
    char output_file_name[4096];
    snprintf(output_file_name, sizeof(output_file_name), 
            "%s/_temporary/map_%d/attempt_%d/part-%05d",
            task.job().output().c_str(),
            task.task_id(),
            task.attempt_id(),
            task.task_id()
            );
    return output_file_name;
}

const std::string Executor::GetMapWorkDir(const TaskInfo& task) {
    char output_file_name[4096];
    snprintf(output_file_name, sizeof(output_file_name), 
            "%s/_temporary/map_%d/attempt_%d",
            task.job().output().c_str(),
            task.task_id(),
            task.attempt_id()
            );
    return output_file_name;
}

const std::string Executor::GetReduceWorkDir(const TaskInfo& task) {
    char output_file_name[4096];
    snprintf(output_file_name, sizeof(output_file_name),
            "%s/_temporary/reduce_%d/attempt_%d",
            task.job().output().c_str(),
            task.task_id(),
            task.attempt_id()
            );
    return output_file_name;
}

const std::string Executor::GetReduceWorkFilename(const TaskInfo& task) {
    char output_file_name[4096];
    snprintf(output_file_name, sizeof(output_file_name), 
            "%s/_temporary/reduce_%d/attempt_%d/part-%05d",
            task.job().output().c_str(),
            task.task_id(),
            task.attempt_id(),
            task.task_id()
            );
    return output_file_name;
}

bool Executor::MoveTempToOutput(const TaskInfo& task, FileSystem* fs, bool is_map) {
    std::string old_name;
    if (is_map) {
        old_name = GetMapWorkFilename(task);
    } else {
        old_name = GetReduceWorkFilename(task);
    }
    char new_name[4096];
    if (task.job().has_compress_output() && task.job().compress_output()) {
        snprintf(new_name, sizeof(new_name), "%s/part-%05d.gz", 
                 task.job().output().c_str(), task.task_id());
    } else {
        snprintf(new_name, sizeof(new_name), "%s/part-%05d", 
                 task.job().output().c_str(), task.task_id());
    }
    
    LOG(INFO, "rename %s -> %s", old_name.c_str(), new_name);
    if (fs->Rename(old_name, new_name)) {
        MoveByPassData(task, fs, is_map);
        return true;
    } else {
        if (fs->Exist(new_name)) {
            LOG(WARNING, "an early attempt has done the task.");
            return true;
        }
        return false;
    }
}

bool Executor::MoveTempToShuffle(const TaskInfo& task) {
    std::string old_dir = GetMapWorkDir(task);
    char new_dir[4096];
    snprintf(new_dir, sizeof(new_dir), 
            "%s/_temporary/shuffle/map_%d",
            task.job().output().c_str(),
            task.task_id());
    FileSystem::Param param;
    FillParam(param, task);
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    boost::scoped_ptr<FileSystem> fs_guard(fs);
    MoveByPassData(task, fs, true);
    LOG(INFO, "rename %s -> %s", old_dir.c_str(), new_dir);
    fs->Rename(old_dir, new_dir);
    if (fs->Exist(new_dir)) {
        return true;
    }
    return false;
}

void Executor::FillParam(FileSystem::Param& param, const TaskInfo& task) {
    if (!task.job().output_dfs().user().empty()) {
        param["user"] = task.job().output_dfs().user();
    }
    if (!task.job().output_dfs().password().empty()) {
        param["password"] = task.job().output_dfs().password();
    }
    if (!task.job().output_dfs().host().empty()) {
        param["host"] = task.job().output_dfs().host();
    }
    if (!task.job().output_dfs().port().empty()) {
        param["port"] = task.job().output_dfs().port();
    }
}

bool Executor::ReadLine(FILE* user_app, std::string* line) {
    char * s = fgets(line_buf_, sLineBufferSize, user_app);
    if (s == NULL && feof(user_app)) {
        return true;
    }
    if (ferror(user_app)) {
        return false;
    }
    *line = line_buf_;
    return true;
}

bool Executor::ReadBlock(FILE* user_app, std::string* block) {
    size_t n_read = fread(line_buf_, 1, 40960, user_app);
    if (n_read == 0 && feof(user_app)) {
        block->erase();
        return true;
    }
    if (ferror(user_app)) {
        return false;
    }
    block->assign(line_buf_, n_read);
    return true;
}

bool Executor::ReadRecord(FILE* user_app, std::string* p_key, std::string* p_value) {
    int32_t key_len = 0;
    int32_t value_len = 0;
    std::string& key = *p_key;
    std::string& value = *p_value;
    if (fread(&key_len, sizeof(key_len), 1, user_app) != 1) {
        if (feof(user_app)) {
            return true;
        }
        LOG(WARNING, "read key_len fail");
        return false;
    }
    if (key_len < 0 || key_len > sKeyLimit) {
        LOG(WARNING, "invalid key len: %d", key_len);
        return false;
    }
    key.resize(key_len);
    if ((int32_t)fread(&key[0], sizeof(char), key_len, user_app) != key_len) {
        LOG(WARNING, "read key fail");
        return false;
    }
    if (fread(&value_len, sizeof(value_len), 1, user_app) != 1) {
        LOG(WARNING, "read value_len fail");
        return false;
    }
    value.resize(value_len);
    if ((int32_t)fread(&value[0], sizeof(char), value_len, user_app) != value_len) {
        LOG(WARNING, "read value fail");
        return false;
    }
    return true;
}

TaskState Executor::TransTextOutput(FILE* user_app, const std::string& temp_file_name,
                                    FileSystem::Param param, const TaskInfo& task) {
    FileSystem* fs = FileSystem::CreateInfHdfs();
    boost::scoped_ptr<FileSystem> fs_guard(fs);
    bool ok = fs->Open(temp_file_name, param, kWriteFile);
    if (!ok) {
        LOG(WARNING, "create output file fail, %s", temp_file_name.c_str());
        return kTaskFailed;
    }

    PipeStyle pipe_style = task.job().pipe_style();
    std::string raw_data;

    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            pclose(user_app);
            return kTaskCanceled;
        }
        if (pipe_style == kStreaming) {
            ok = ReadBlock(user_app, &raw_data);
        } else if (pipe_style == kBiStreaming) {
            std::string key;
            std::string value;
            ok = ReadRecord(user_app, &key, &value);
            raw_data = key + "\t" + value + "\n";
        } else {
            LOG(FATAL, "unkonow pipe_style: %d", pipe_style);
        }
        if (!ok) {
            LOG(WARNING, "read app output fail");
            return kTaskFailed;
        }
        ok = fs->WriteAll((void*)raw_data.data(), raw_data.size());
        if (!ok) {
            LOG(WARNING, "write output to dfs fail");
            return kTaskFailed;
        }
    }
    ok = fs->Close();
    if (!ok) {
        LOG(WARNING, "close file fail: %s", temp_file_name.c_str());
        return kTaskFailed;
    }
    return kTaskCompleted;
}

TaskState Executor::TransBinaryOutput(FILE* user_app, const std::string& temp_file_name,
                                      FileSystem::Param param, const TaskInfo& task) {
    InfSeqFile seqfile;
    if (!seqfile.Open(temp_file_name, param, kWriteFile)) {
        LOG(WARNING, "fail to open %s for wirte", temp_file_name.c_str());
        return kTaskFailed;
    }
    PipeStyle pipe_style = task.job().pipe_style();
    bool ok = false;
    std::string key;
    std::string value;
    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            pclose(user_app);
            return kTaskCanceled;
        }
        if (pipe_style == kStreaming) {
            ok = ReadLine(user_app, &value);
        } else if (pipe_style == kBiStreaming) {
            ok = ReadRecord(user_app, &key, &value);
        } else {
            LOG(FATAL, "invalid pipe style: %d", pipe_style);
        }
        if (feof(user_app)) {
            LOG(INFO, "read user app over");
            break;
        }
        if (!ok) {
            LOG(INFO, "read user app fail");
            return kTaskFailed;
        }
        ok = seqfile.WriteNextRecord(key, value);
        if (!ok) {
            LOG(WARNING, "fail to write: %s", temp_file_name.c_str());
            return kTaskFailed;
        }
    }

    if (!seqfile.Close()) {
        LOG(WARNING, "fail to close %s", temp_file_name.c_str());
        return kTaskFailed;
    }
    return kTaskCompleted;
}


TaskState Executor::TransMultipleTextOutput(FILE* user_app, const std::string& temp_file_name,
                                            FileSystem::Param param, const TaskInfo& task) {
    boost::scoped_ptr<FileSystem> fs_array[26];
    PipeStyle pipe_style = task.job().pipe_style();
    std::string raw_data;
    int offset = 0;
    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            pclose(user_app);
            return kTaskCanceled;
        }
        if (pipe_style == kStreaming) {
            std::string line;
            bool ok = ReadLine(user_app, &line); //contains \n
            if (!ok) {
                LOG(WARNING, "read app output fail");
                return kTaskFailed;
            }
            if (feof(user_app)) {
                LOG(INFO, "read user app over");
                break;
            }
            if (line.size() < 2) {
                continue;
            }
            if (line[line.size() - 1] == '\n') {
                line.erase(line.end() - 1);
            }
            if (line.size() < 2) {
                continue;
            }
            char suffix = line[line.size() - 1];
            if (suffix < 'A' || suffix > 'Z') {
                continue;
            }
            offset = suffix - 'A';
            raw_data = line.substr(0, line.size() - 2) + "\n";
        } else if (pipe_style == kBiStreaming) {
            std::string key;
            std::string value;
            bool ok = ReadRecord(user_app, &key, &value);
            if (!ok) {
                LOG(WARNING, "read app output fail");
                return kTaskFailed;
            }
            if (feof(user_app)) {
                LOG(INFO, "read user app over");
                break;
            }
            if (value.size() < 2) { // e.g. "...#A", length is at least 2
                continue;
            }
            char suffix = value[value.size() - 1];
            if (suffix < 'A' || suffix > 'Z') {
                continue;
            }
            offset = suffix - 'A';
            value.erase(value.end() - 2, value.end());
            raw_data = key + "\t" + value + "\n";
        } else {
            LOG(FATAL, "unkonow pipe_style: %d", pipe_style);
        }
        if (fs_array[offset].get() == NULL) {
            FileSystem* fs = FileSystem::CreateInfHdfs();
            fs_array[offset].reset(fs);
            char suffix = 'A' + offset;
            std::string real_name = temp_file_name + "-" + suffix;
            bool ok = fs->Open(real_name, param, kWriteFile);
            if (!ok) {
                LOG(WARNING, "create output file fail, %s", real_name.c_str());
                return kTaskFailed;
            }
        }
        bool ok = fs_array[offset]->WriteAll((void*)raw_data.data(), raw_data.size());
        if (!ok) {
            LOG(WARNING, "write output to dfs fail");
            return kTaskFailed;
        }
    }
    for (int i = 0; i < 26; i++) {
        if (fs_array[i].get() == NULL) {
            continue;
        }
        bool ok = fs_array[i]->Close();
        if (!ok) {
            LOG(WARNING, "close file fail: %s", temp_file_name.c_str());
            return kTaskFailed;
        }
    }
    return kTaskCompleted;
}

void Executor::UploadErrorMsg(const TaskInfo& task, bool is_map, const std::string& error_msg) {
    std::string log_name;
    std::string work_dir = task.job().output() + "/_temporary";
    if (is_map) {
        char output_file_name[4096];
        snprintf(output_file_name, sizeof(output_file_name),
                "%s/_temporary/errors/map_%d/attempt_%d.log",
                task.job().output().c_str(),
                task.task_id(),
                task.attempt_id()
                );
        log_name = output_file_name;
    } else {
        char output_file_name[4096];
        snprintf(output_file_name, sizeof(output_file_name),
                "%s/_temporary/errors/reduce_%d/attempt_%d.log",
                task.job().output().c_str(),
                task.task_id(),
                task.attempt_id()
                );
        log_name = output_file_name;
    }
    FileSystem::Param param;
    FillParam(param, task);
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    boost::scoped_ptr<FileSystem> fs_guard(fs);
    if (fs->Exist(work_dir) && fs->Open(log_name, param, kWriteFile)) {
        fs->WriteAll((void*)&error_msg[0], error_msg.size());
        if (!fs->Close()) {
            LOG(WARNING, "fail to upload errors to hdfs");
        }
    }
}

std::string Executor::GetErrorMsg(const TaskInfo& task, bool is_map) {
    std::stringstream cmd_ss;
    std::stringstream task_local_dir;
    std::string task_type = (is_map ? "map_" : "reduce_");
    task_local_dir << task_type << task.task_id() << "_" << task.attempt_id();
    cmd_ss << "ls -tr " << task_local_dir.str() << "/* | grep -P 'stdout|stderr|\\.log' | "
              "while read f_name ;do  echo $f_name && tail -c 8000 $f_name; done | tail -c 8000";
    LOG(INFO, "=== upload task log ===");
    LOG(INFO, "%s", cmd_ss.str().c_str());
    FILE* reporter = popen(cmd_ss.str().c_str(), "r");
    std::string err_msg;
    std::string line;
    while (ReadLine(reporter, &line)) {
        if (feof(reporter)) {
            break;
        }
        err_msg += line;
    }
    pclose(reporter);
    return err_msg;
}

bool Executor::MoveMultipleTempToOutput(const TaskInfo& task, FileSystem* fs, bool is_map) {
    std::string old_name;
    if (is_map) {
        old_name = GetMapWorkFilename(task);
    } else {
        old_name = GetReduceWorkFilename(task);
    }
    for (int i = 0; i < 26; i++) {
        char suffix = 'A' + i;
        std::string real_old_name = old_name + "-" + suffix;
        if (!fs->Exist(real_old_name)) {
            continue;
        }
        char new_name[4096];
        if (task.job().has_compress_output() && task.job().compress_output()) {
            snprintf(new_name, sizeof(new_name), "%s/part-%05d-%c.gz", 
                     task.job().output().c_str(), task.task_id(), suffix);
        } else {
            snprintf(new_name, sizeof(new_name), "%s/part-%05d-%c", 
                     task.job().output().c_str(), task.task_id(), suffix);    
        }
        
        LOG(INFO, "rename %s -> %s", real_old_name.c_str(), new_name);
        if (fs->Rename(real_old_name, new_name)) {
            continue;
        } else {
            if (fs->Exist(new_name)) {
                LOG(WARNING, "an early attempt has done the task.");
                continue;
            }
            return false;
        }
    }
    MoveByPassData(task, fs, is_map);
    return true;
}

bool Executor::MoveByPassData(const TaskInfo& task, FileSystem* fs, bool is_map) {
    std::string tmp_dir;
    if (is_map) {
        tmp_dir = GetMapWorkDir(task);
    } else {
        tmp_dir = GetReduceWorkDir(task);
    }
    std::vector<FileInfo> children;
    if (!fs->List(tmp_dir, &children)) {
        return false;
    }
    for (size_t i = 0; i < children.size(); i++) {
        const FileInfo& child_dir = children[i];
        if(child_dir.kind != 'D') {
            continue;
        }
        //sub-directory
        std::vector<FileInfo> bypass_files;
        if (!fs->List(child_dir.name, &bypass_files)) {
            continue;
        }
        for (size_t j = 0; j < bypass_files.size(); j++) {
            const FileInfo& bypass_file = bypass_files[j];
            if (bypass_file.kind == 'F') {
                size_t pos1 = child_dir.name.rfind("/");
                std::string short_dir_name = child_dir.name.substr(pos1);
                std::string short_file_name = 
                    bypass_file.name.substr(child_dir.name.size() + 1);
                std::string new_name = 
                    task.job().output() + "/" + short_dir_name + "/" + short_file_name;
                if (!fs->Exist(task.job().output() + "/" + short_dir_name)) {
                    fs->Mkdirs(task.job().output() + "/" + short_dir_name);
                }
                LOG(INFO, "rename bypass: %s -> %s",
                    bypass_file.name.c_str(), new_name.c_str());
                fs->Rename(bypass_file.name, new_name);
            }
        }
    }
    return true;
}

bool Executor::ParseCounters(const TaskInfo& task,
                             std::map<std::string, int64_t>* counters,
                             bool is_map) {
    assert(counters);
    std::stringstream cmd_ss;
    std::stringstream task_local_dir;
    std::string task_type = (is_map ? "map_" : "reduce_");
    task_local_dir << task_type << task.task_id() << "_" << task.attempt_id();
    std::string task_stderr_name = task_local_dir.str() + "/stderr";
    FILE* task_stderr_file = fopen(task_stderr_name.c_str(), "r");
    if (!task_stderr_file) {
        LOG(WARNING, "failed to read stderr of this task");
        return false;
    }
    std::string line;
    while (ReadLine(task_stderr_file, &line)) {
        if (feof(task_stderr_file)) {
            break;
        }
        if (counters->size() >= sMaxCounters) {
            break;
        }
        if (!boost::starts_with(line, "reporter:counter:")) {
            continue;
        }
        size_t value_idx = line.rfind(",");
        size_t key_idx = line.rfind(":");
        int64_t value;
        std::string key;
        if (value_idx == std::string::npos || 
            key_idx == std::string::npos ||
            key_idx >= value_idx) {
            continue;
        }
        key = line.substr(key_idx+1, value_idx - key_idx - 1);
        if (key.empty()) {
            continue;
        }
        std::string s_value = line.substr(value_idx+1);
        sscanf(s_value.c_str(), "%lld", (long long int*)&value);
        (*counters)[key] += value;
    }
    fclose(task_stderr_file);
    return true;
}

} //namespace shuttle
} //namespace baidu

