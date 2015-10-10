#ifndef _BAIDU_SHUTTLE_EXECUTOR_H_
#define _BAIDU_SHUTTLE_EXECUTOR_H_

#include <logging.h>
#include <string>
#include <vector>
#include <utility>
#include <set>
#include "sort/filesystem.h"
#include "proto/shuttle.pb.h"
#include "mutex.h"

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

class Executor {
public:
    virtual ~Executor();
    static Executor* GetExecutor(WorkMode mode);
    void SetEnv(const std::string& jobid, const TaskInfo& task);
    virtual TaskState Exec(const TaskInfo& task) = 0;
    void Stop(int32_t task_id);
protected:
    Executor() ;
    bool ShouldStop(int32_t task_id);
    const std::string GetMapWorkFilename(const TaskInfo& task);
    const std::string GetReduceWorkFilename(const TaskInfo& task);
    const std::string GetMapWorkDir(const TaskInfo& task);
    bool MoveTempToOutput(const TaskInfo& task, FileSystem* fs, bool is_map);
    bool MoveTempToShuffle(const TaskInfo& task);
    const std::string GetShuffleWorkDir(const TaskInfo& task);
private:
    std::set<int32_t> stop_task_ids_;
    Mutex mu_;
};

class MapExecutor : public Executor {
public:
    MapExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~MapExecutor();
private:
    char* line_buf_;
};

class ReduceExecutor : public Executor {
public:
    ReduceExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~ReduceExecutor();
};

class MapOnlyExecutor : public Executor {
public:
    MapOnlyExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~MapOnlyExecutor();
};

}
}

#endif
