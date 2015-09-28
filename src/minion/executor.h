#ifndef _BAIDU_SHUTTLE_EXECUTOR_H_
#define _BAIDU_SHUTTLE_EXECUTOR_H_

#include <logging.h>
#include "sort/filesystem.h"
#include "proto/shuttle.pb.h"

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
    const std::string GetMapOutputFilename(const TaskInfo& task);
    const std::string GetReduceOutputFilename(const TaskInfo& task);
    bool MoveTempToOutput(const TaskInfo& task, FileSystem* fs, bool is_map);
    virtual TaskState Exec(const TaskInfo& task) = 0;
protected:
    Executor() ;
};

class MapExecutor : public Executor {
public:
    MapExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~MapExecutor();
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
