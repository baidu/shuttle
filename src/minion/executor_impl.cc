#include "executor.h"
#include <unistd.h>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>

namespace baidu {
namespace shuttle {

Executor::Executor() {
    
}

Executor::~Executor() {

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
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    LOG(INFO, "rename %s -> %s", old_dir.c_str(), new_dir);
    bool ret = fs->Rename(old_dir, new_dir);
    delete fs;
    return ret;
}

void Executor::FillParam(FileSystem::Param& param, const TaskInfo& task) {
    if (!task.job().output_dfs().user().empty()) {
        param["host"] = task.job().output_dfs().host();
        param["port"] = task.job().output_dfs().port();
        param["user"] = task.job().output_dfs().user();
        param["password"] = task.job().output_dfs().password();
    }
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

    const size_t buf_size = 40960;
    char* buf = (char*) malloc(buf_size);
    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            free(buf);
            return kTaskCanceled;
        }
        size_t n_read = fread(buf, sizeof(char), buf_size, user_app);
        if (n_read != buf_size) {
            if (feof(user_app)) {
                ok = fs->WriteAll(buf, n_read);
                break;
            } else if (ferror(user_app) != 0) {
                LOG(WARNING, "errors occur in reading, %s", strerror(errno));
                ok = false;
                break;
            } else {
                assert(0);
            }
        }
        ok = fs->WriteAll(buf, n_read);
        if (!ok) {
            break;
        }
    }
    free(buf);
    if (!ok || !fs->Close()) {
        LOG(WARNING, "write data fail, %s", temp_file_name.c_str());
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
    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            return kTaskCanceled;
        }
        int32_t key_len = 0;
        int32_t value_len = 0;
        std::string key;
        std::string value;
        if (fread(&key_len, sizeof(key_len), 1, user_app) != 1) {
            if (feof(user_app)) {
                break;
            }
            LOG(WARNING, "read key_len fail");
            return kTaskFailed;
        }
        if (key_len < 0 || key_len > 65536) {
            LOG(WARNING, "invalid key len: %d", key_len);
            return kTaskFailed;
        }
        key.resize(key_len);
        if ((int32_t)fread((void*)key.data(), sizeof(char), key_len, user_app) != key_len) {
            LOG(WARNING, "read key fail");
            return kTaskFailed;
        }
        if (fread(&value_len, sizeof(value_len), 1, user_app) != 1) {
            LOG(WARNING, "read value_len fail");
            return kTaskFailed;
        }
        value.resize(value_len);
        if ((int32_t)fread((void*) value.data(), sizeof(char), value_len, user_app) != value_len) {
            LOG(WARNING, "read value fail");
            return kTaskFailed;
        }
        bool ok = seqfile.WriteNextRecord(key, value);
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

} //namespace shuttle
} //namespace baidu

