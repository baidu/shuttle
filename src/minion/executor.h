#ifndef _BAIDU_SHUTTLE_EXECUTOR_H_
#define _BAIDU_SHUTTLE_EXECUTOR_H_

#include <logging.h>
#include <string>
#include <vector>
#include <utility>
#include <set>
#include "common/filesystem.h"
#include "proto/shuttle.pb.h"
#include "mutex.h"

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

const int sLineBufferSize = 4096000;
const int sKeyLimit = 65536;
const size_t sMaxCounters = 10000;

class Partitioner;
class Emitter;

class Executor {
public:
    virtual ~Executor();
    static Executor* GetExecutor(WorkMode mode);
    void SetEnv(const std::string& jobid, const TaskInfo& task, WorkMode mode);
    virtual TaskState Exec(const TaskInfo& task) = 0;
    void Stop(int32_t task_id);
    std::string GetErrorMsg(const TaskInfo& task, bool is_map);
    void UploadErrorMsg(const TaskInfo& task, bool is_map, const std::string& error_msg);
    static void FillParam(FileSystem::Param& param, const TaskInfo& task);
    bool ParseCounters(const TaskInfo& task,
                       std::map<std::string, int64_t>* counters,
                       bool is_map);
protected:
    Executor() ;
    bool ShouldStop(int32_t task_id);
    const std::string GetMapWorkFilename(const TaskInfo& task);
    const std::string GetReduceWorkFilename(const TaskInfo& task);
    const std::string GetMapWorkDir(const TaskInfo& task);
    const std::string GetReduceWorkDir(const TaskInfo& task);
    bool MoveTempToOutput(const TaskInfo& task, FileSystem* fs, bool is_map);
    bool MoveTempToShuffle(const TaskInfo& task);
    bool MoveByPassData(const TaskInfo& task, FileSystem* fs, bool is_map);
    const std::string GetShuffleWorkDir(const TaskInfo& task);

    bool ReadLine(FILE* user_app, std::string* line);
    bool ReadRecord(FILE* user_app, std::string* key, std::string* value);
    bool ReadBlock(FILE* user_app, std::string* line);

    TaskState TransTextOutput(FILE* user_app, const std::string& temp_file_name,
                              FileSystem::Param param, const TaskInfo& task);
    TaskState TransBinaryOutput(FILE* user_app, const std::string& temp_file_name,
                                FileSystem::Param param, const TaskInfo& task);
    TaskState TransMultipleTextOutput(FILE* user_app, const std::string& temp_file_name,
                                      FileSystem::Param param, const TaskInfo& task);
    bool MoveMultipleTempToOutput(const TaskInfo& task, FileSystem* fs, bool is_map);

protected:
    char* line_buf_;

private:
    std::set<int32_t> stop_task_ids_;
    Mutex mu_;

};

class MapExecutor : public Executor {
public:
    MapExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~MapExecutor();
    TaskState StreamingShuffle(FILE* user_app, const TaskInfo& task,
                              const Partitioner* partitioner, Emitter* emitter);
    TaskState BiStreamingShuffle(FILE* user_app, const TaskInfo& task,
                                const Partitioner* partitioner, Emitter* emitter);
};

class ReduceExecutor : public Executor {
public:
    ReduceExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~ReduceExecutor();
};

class MapOnlyExecutor: public Executor {
public:
    MapOnlyExecutor();
    virtual TaskState Exec(const TaskInfo& task);
    virtual ~MapOnlyExecutor();
};

}
}

#endif
