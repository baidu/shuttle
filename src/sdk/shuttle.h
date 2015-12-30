#ifndef _BAIDU_SHUTTLE_SDK_SHUTTLE_H_
#define _BAIDU_SHUTTLE_SDK_SHUTTLE_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include <string>

namespace baidu {
namespace shuttle {
namespace sdk {

enum JobState {
    kPending = 0,
    kRunning = 1,
    kFailed = 2,
    kKilled = 3,
    kCompleted = 4
};

enum JobPriority {
    kVeryHigh = 0,
    kHigh = 1,
    kNormal = 2,
    kLow = 3,
    // In update interface, the undefined
    //     means not to update priority
    // Otherwise, it has the exact meaning
    //     as kNormal
    kUndefined = 10
};

enum TaskState {
    kTaskPending = 0,
    kTaskRunning = 1,
    kTaskFailed = 2,
    kTaskKilled = 3,
    kTaskCompleted = 4,
    kTaskCanceled = 5,
    kTaskUnknown = 10
};

enum PartitionMethod {
    kKeyFieldBased = 0,
    kIntHash = 1
};

enum InputFormat {
    kTextInput = 0,
    kBinaryInput = 1,
    kNLineInput = 2
};

enum OutputFormat {
    kTextOutput = 0,
    kBinaryOutput = 1,
    kSuffixMultipleTextOutput = 2
};

enum TaskType {
    kMap = 0,
    kReduce = 1,
    kMapOnly = 2
};

enum PipeStyle {
    kStreaming = 0,
    kBiStreaming = 1
};

struct TaskStatistics {
    int32_t total;
    int32_t pending;
    int32_t running;
    int32_t failed;
    int32_t killed;
    int32_t completed;
};

struct DfsInfo {
    std::string host;
    std::string port;
    std::string user;
    std::string password;
};

struct JobDescription {
    std::string name;
    std::string user;
    JobPriority priority;
    int32_t map_capacity;
    int32_t reduce_capacity;
    int32_t millicores;
    int64_t memory;
    std::vector<std::string> files;
    std::vector<std::string> inputs;
    std::string output;
    std::string map_command;
    std::string reduce_command;
    PartitionMethod partition;
    int32_t map_total;
    int32_t reduce_total;
    std::string key_separator;
    int32_t key_fields_num;
    int32_t partition_fields_num;
    DfsInfo input_dfs;
    DfsInfo output_dfs;
    InputFormat input_format;
    OutputFormat output_format;
    PipeStyle pipe_style;
    bool map_allow_duplicates;
    bool reduce_allow_duplicates;
    int32_t map_retry;
    int32_t reduce_retry;
    int64_t split_size;
    bool check_counters;
};

struct TaskInstance {
    std::string job_id;
    int32_t task_id;
    int32_t attempt_id;
    std::string input_file;
    TaskState state;
    TaskType type;
    std::string minion_addr;
    float progress;    
    time_t start_time;
    time_t end_time;
};

struct JobInstance {
    JobDescription desc;
    std::string jobid;
    JobState state;
    TaskStatistics map_stat;
    TaskStatistics reduce_stat;
    int32_t start_time;
    int32_t finish_time;
};

} //namspace sdk

class Shuttle {
public:
    static Shuttle* Connect(const std::string& master_addr);

    virtual bool SubmitJob(const sdk::JobDescription& job_desc,
                           std::string& job_id) = 0;
    virtual bool UpdateJob(const std::string& job_id,
                           const sdk::JobPriority& priority,
                           const int map_capacity,
                           const int reduce_capacity) = 0;
    virtual bool KillJob(const std::string& job_id) = 0;
    virtual bool KillTask(const std::string& job_id, sdk::TaskType mode,
                          int task_id, int attempt_id) = 0;
    virtual bool ShowJob(const std::string& job_id, 
                         sdk::JobInstance& job,
                         std::vector<sdk::TaskInstance>& tasks,
                         bool display_all,
                         bool show_detail,
                         std::string& error_msg) = 0;
    virtual bool ListJobs(std::vector<sdk::JobInstance>& jobs,
                          bool display_all = true) = 0;
    virtual void SetRpcTimeout(int timeout) = 0;
};

} //namespace shuttle
} //namespace baidu

#endif
