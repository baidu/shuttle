#include "executor.h"
#include <unistd.h>
#include <boost/lexical_cast.hpp>

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
    std::string shuffle_work_dir = task.job().output() + "/_temporary/shuffle";
    ::setenv("shuffle_work_dir", shuffle_work_dir.c_str(), 1);
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

} //namespace shuttle
} //namespace baidu

