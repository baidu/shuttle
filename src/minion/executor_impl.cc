#include "executor.h"
#include <unistd.h>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>

namespace baidu {
namespace shuttle {

const int sLineBufferSize = 40960;
const int sKeyLimit = 65536;

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

void Executor::SetEnv(const std::string& jobid, const TaskInfo& task) {
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
    if (task.job().input_format() == kTextInput) {
        ::setenv("minion_input_format", "text", 1);
    } else if (task.job().input_format() == kBinaryInput) {
        ::setenv("minion_input_format", "binary", 1);
    }
    if (task.job().output_format() == kTextOutput) {
        ::setenv("minion_output_format", "text", 1);
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
    snprintf(new_name, sizeof(new_name), "%s/part-%05d", 
             task.job().output().c_str(), task.task_id());
    LOG(INFO, "rename %s -> %s", old_name.c_str(), new_name);
    return fs->Rename(old_name, new_name);
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
    LOG(INFO, "rename %s -> %s", old_dir.c_str(), new_dir);
    bool ret = fs->Rename(old_dir, new_dir);
    delete fs;
    return ret;
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
            std::string line;
            ok = ReadLine(user_app, &line);
            raw_data = line;
        } else if (pipe_style == kBiStreaming) {
            std::string key;
            std::string value;
            ok = ReadRecord(user_app, &key, &value);
            raw_data = key + "\t" + value + "\n";
        } else {
            LOG(FATAL, "unkonow pipe_style: %d", pipe_style);
        }
        if (feof(user_app)) {
            LOG(INFO, "read user app over");
            break;
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

void Executor::ReportErrors(const TaskInfo& task, bool is_map) {
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
    FileSystem* fs = FileSystem::CreateInfHdfs();
    boost::scoped_ptr<FileSystem> fs_guard(fs);
    FileSystem::Param param;
    FillParam(param, task);
    if (fs->Exist(work_dir) && fs->Open(log_name, param, kWriteFile)) {
        FILE* reporter = popen("ls -tr | grep -P 'stdout_|stderr_|\\.log' | "
                               "while read f_name ;do  echo $f_name && cat $f_name; done | tail -20000", "r");
        std::string line;
        while (ReadLine(reporter, &line)) {
            if (feof(reporter)) {
                break;
            }
            fs->WriteAll(&line[0], line.size());
        }
        pclose(reporter);
        if (!fs->Close()) {
            LOG(WARNING, "fail to report errors to hdfs");
        }
    }
}

} //namespace shuttle
} //namespace baidu

