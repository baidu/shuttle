#ifndef _BAIDU_SHUTTLE_SDK_SHUTTLE_H_
#define _BAIDU_SHUTTLE_SDK_SHUTTLE_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include <map>
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

enum WorkMode {
    kMap = 0,
    kReduce = 1
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
    std::string path;
    std::string host;
    std::string port;
    std::string user;
    std::string password;
};

struct NodeConfig {
    int32_t node;
    WorkMode type;
    int32_t capacity;
    int32_t total;
    int32_t millicores;
    int64_t memory;
    std::string command;
    std::vector<DfsInfo> inputs;
    InputFormat input_format;
    DfsInfo output;
    OutputFormat output_format;
    PartitionMethod partition;
    std::string key_separator;
    int32_t key_fields_num;
    int32_t partition_fields_num;
    bool allow_duplicates;
    int32_t retry;
    std::string combiner;
    bool check_counters;
    int32_t ignore_failures;
    bool decompress_input;
    bool compress_output;
    std::vector<std::string> cmdenvs;
};

struct JobDescription {
    std::string name;
    std::vector<std::string> files;
    std::string cache_archive;
    PipeStyle pipe_style;
    int64_t split_size;
    std::vector<NodeConfig> nodes;
    std::vector< std::vector<int32_t> > map;
};

struct TaskInstance {
    std::string job_id;
    int32_t node;
    int32_t task_id;
    int32_t attempt_id;
    std::string input_file;
    TaskState state;
    std::string minion_addr;
    float progress;    
    time_t start_time;
    time_t finish_time;
};

struct JobInstance {
    JobDescription desc;
    std::string jobid;
    JobState state;
    std::vector<TaskStatistics> stats;
    int32_t start_time;
    int32_t finish_time;
};

} // namspace sdk

class Shuttle {
public:
    static Shuttle* Connect(const std::string& master_addr);

    virtual bool SubmitJob(const sdk::JobDescription& job_desc,
                           std::string& job_id) = 0;
    virtual bool UpdateJob(const std::string& job_id,
                           const std::map<int32_t, int32_t>& new_capacities) = 0;
    virtual bool KillJob(const std::string& job_id) = 0;
    virtual bool KillTask(const std::string& job_id, int node,
                          int task_id, int attempt_id) = 0;
    virtual bool ShowJob(const std::string& job_id, 
                         sdk::JobInstance& job,
                         std::vector<sdk::TaskInstance>& tasks,
                         bool display_all = true) = 0;
    virtual bool ListJobs(std::vector<sdk::JobInstance>& jobs,
                          bool display_all = true) = 0;
    virtual void SetRpcTimeout(int timeout) = 0;
};

} // namespace shuttle
} // namespace baidu

#endif

