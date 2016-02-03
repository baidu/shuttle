#include <string>
#include <vector>
#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <fstream>

#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>

#include "sdk/shuttle.h"
#include "ins_sdk.h"
#include "common/table_printer.h"

namespace config {

std::vector<std::string> params;

std::string input;
std::string output;
std::string file;
std::string map;
std::string reduce;
std::string jobconf;
std::string nexus;
std::string nexus_root = "/shuttle/";
std::string nexus_file;
std::string master = "master";

bool display_all = false;
bool immediate_return = false;

std::string job_name = "map_reduce_job";
::baidu::shuttle::sdk::JobPriority job_priority = \
    ::baidu::shuttle::sdk::kUndefined;
::baidu::shuttle::sdk::PartitionMethod partitioner = \
    ::baidu::shuttle::sdk::kKeyFieldBased;
::baidu::shuttle::sdk::InputFormat input_format = \
    ::baidu::shuttle::sdk::kTextInput;
::baidu::shuttle::sdk::OutputFormat output_format = \
    ::baidu::shuttle::sdk::kTextOutput;
::baidu::shuttle::sdk::PipeStyle pipe_style = \
    ::baidu::shuttle::sdk::kStreaming;
int job_cpu = 1000;
int64_t job_memory = 1024l * 1024 * 1024;
int map_capacity = -1; // default value assigned during submitting
int reduce_capacity = -1; // default value assigned during submitting
int map_tasks = 5000;
int reduce_tasks = 1;
std::string key_separator = "\t";
int key_fields_num = 1;
int partition_fields_num = 1;
std::string input_host;
std::string input_port;
std::string input_user;
std::string input_password;
std::string output_host;
std::string output_port;
std::string output_user;
std::string output_password;
bool map_speculative_exec = true;
bool reduce_speculative_exec = true;
int map_retry = 3;
int reduce_retry = 3;
int64_t split_size = 500l * 1024 * 1024;
std::string err_msg;
bool check_counters = false;
}

const std::string error_message = "shuttle client - A fast computing framework base on Galaxy\n"
        "Usage:\n"
        "\tshuttle streaming/bistreaming [flags]\n"
        "\tshuttle update <jobid> [new flags]\n"
        "\tshuttle kill <jobid> [<map/reduce/m/r>-<task id>-<attempt id>]\n"
        "\tshuttle list\n"
        "\tshuttle status <jobid>\n"
        "\tshuttle monitor <jobid>\n"
        "Options:\n"
        "\t-h  --help\t\t\tShow this information\n"
        "\t-a  --all\t\t\tConsider finished and dead jobs in status and list operation\n"
        "\t-i  --immediate\tStop monitoring the state of submitted job and return immediately\n"
        "\t-input <file>\t\t\tSpecify the input file, using a hdfs path\n"
        "\t-output <path>\t\t\tSpecify the output path, which must be empty\n"
        "\t-file <file>[,...]\t\tSpecify the files needed by your program\n"
        "\t-cacheArchive <file>\\#<dir>\tSpecify the additional package and its relative path\n"
        "\t-mapper <file>\t\t\tSpecify the map program\n"
        "\t-reducer <file>\t\t\tSpecify the reduce program\n"
        "\t-partitioner <partitioner>\tSpecify the partitioner used when shuffling\n"
        "\t-inputformat <input format>\tSpecify the input format\n"
        "\t-outputformat <output format>\tSpecify the output format\n"
        "\t-jobconf <key>=<value>[,...]\tSpecify the configuration of the job\n"
        "\t  mapred.job.name\t\tName the submitting job\n"
        "\t  mapred.job.priority\t\tSpecify the priority of the job\n"
        "\t  mapred.job.cpu.millicores\tSpecify the cpu resource occuptation\n"
        "\t  mapred.job.memory.limit\tSpecify the memory resource occuptation\n"
        "\t  mapred.job.map.capacity\tSpecify the slot number that map tasks can use\n"
        "\t  mapred.job.reduce.capacity\tSpecify the slot number that reduce tasks can use\n"
        "\t  mapred.job.input.host\t\tSpecify the host of the dfs which stores inputs\n"
        "\t  mapred.job.input.port\t\tSpecify the port, ditto\n"
        "\t  mapred.job.input.user\t\tSpecify the user, ditto\n"
        "\t  mapred.job.input.password\tSpecify the password, ditto\n"
        "\t  mapred.job.output.host\tSpecify the host of the dfs which stores output\n"
        "\t  mapred.job.output.port\tSpecify the port, ditto\n"
        "\t  mapred.job.output.user\tSpecify the user, ditto\n"
        "\t  mapred.job.output.password\tSpecify the password, ditto\n"
        "\t  mapred.map.max.attempts\t\tSpecify the number of map tasks\n"
        "\t  mapred.job.check.counters\t\tEnable checking job counters\n"
        "\t  mapred.reduce.max.attempts\t\tSpecify the number of reduce tasks\n"
        "\t  mapred.map.tasks\t\tSpecify the number of map tasks\n"
        "\t  mapred.reduce.tasks\t\tSpecify the number of reduce tasks\n"
        "\t  mapred.map.tasks.speculative.execution\tAllow if shuttle can speculatively decide attempts of a map\n"
        "\t  mapred.reduce.tasks.speculative.execution\tDitto on reduce tasks\n"
        "\t  mapred.input.split.size\t\tSpecify the block size to divide the input files\n"
        "\t  map.key.field.separator\tSpecify the separator for key field in shuffling\n"
        "\t  stream.num.map.output.key.fields\tSpecify the output fields number of key after mapper\n"
        "\t  num.key.fields.for.partition\tSpecify the first n fields in key in partitioning\n"
        "\t-nexus <servers>[,...]\t\tSpecify the hosts of nexus server\n"
        "\t-nexus-file <file>\t\tSpecify the flag file used by nexus, will override the option above\n"
        "\t-nexus-root <path>\t\tSpecify the root path of nexus\n"
        "\t-master <path>\t\t\tSpecify the master path in nexus\n"
;

static const char* priority_string[] = {
    "Very High", "High",
    "Normal", "Low"
};

static const char* state_string[] = {
    "Pending", "Running", "Failed",
    "Killed", "Completed", "Canceled"
};

static const std::string task_type_string[] = {
    "map", "reduce", "map"
};

static bool done = false;

struct TaskComparator {
    bool operator()(const ::baidu::shuttle::sdk::TaskInstance& task1,
                    const ::baidu::shuttle::sdk::TaskInstance& task2) const {
        if (task1.type != task2.type) {
            return task1.type == ::baidu::shuttle::sdk::kMap;
        }
        if (task1.task_id != task2.task_id) {
            return task1.task_id < task2.task_id;
        }
        return task1.attempt_id < task2.attempt_id;
    }
};

struct JobComparator {
    bool operator()(const ::baidu::shuttle::sdk::JobInstance& job1,
                    const ::baidu::shuttle::sdk::JobInstance& job2) const {
        if (job1.state == ::baidu::shuttle::sdk::kRunning && 
            job2.state != ::baidu::shuttle::sdk::kRunning) {
            return true;
        }
        if (job1.state != ::baidu::shuttle::sdk::kRunning && 
            job2.state == ::baidu::shuttle::sdk::kRunning) {
            return false;
        }
        return job1.jobid > job2.jobid;
    }
};

static inline ::baidu::shuttle::sdk::PartitionMethod
ParsePartitioner(const std::string& partitioner) {
    if (boost::iequals(partitioner, "keyfieldbased") ||
            boost::iequals(partitioner, "keyfieldbasedpartitioner")) {
        return ::baidu::shuttle::sdk::kKeyFieldBased;
    } else if (boost::iequals(partitioner, "inthash") ||
            boost::iequals(partitioner, "inthashpartitioner")) {
        return ::baidu::shuttle::sdk::kIntHash;
    }
    return ::baidu::shuttle::sdk::kKeyFieldBased;
}

static inline ::baidu::shuttle::sdk::InputFormat
ParseInputFormat(const std::string& input_format) {
    if (boost::starts_with(input_format, "Text")) {
        return ::baidu::shuttle::sdk::kTextInput;
    } else if (boost::starts_with(input_format, "Binary") ||
            boost::starts_with(input_format, "SequenceFile")) {
        return ::baidu::shuttle::sdk::kBinaryInput;
    } else if (boost::starts_with(input_format, "NLine")) {
        return ::baidu::shuttle::sdk::kNLineInput;
    }
    return ::baidu::shuttle::sdk::kTextInput;
}

static inline ::baidu::shuttle::sdk::OutputFormat
ParseOutputFormat(const std::string& output_format) {
    if (boost::starts_with(output_format, "Text")) {
        return ::baidu::shuttle::sdk::kTextOutput;
    } else if (boost::starts_with(output_format, "Binary") ||
            boost::starts_with(output_format, "SequenceFile")) {
        return ::baidu::shuttle::sdk::kBinaryOutput;
    } else if (boost::starts_with(output_format, "SuffixMultipleText")) {
        return ::baidu::shuttle::sdk::kSuffixMultipleTextOutput;
    }
    return ::baidu::shuttle::sdk::kTextOutput;
}

static bool ParseBooleanValue(const std::string& boolean) {
    if (boost::iequals(boolean, "true") ||
            boost::iequals(boolean, "1")) {
        return true;
    } else if (boost::iequals(boolean, "false") ||
            boost::iequals(boolean, "0")) {
        return false;
    }
    return true;
}

static int ParseCommandLineFlags(int* argc, char***argv) {
    char **opt = *argv;
    char *ctx = NULL;
    int cnt = *argc;
    int ret = 0;
    for (int i = 1; i < cnt; ++i) {
        if (opt[i][0] != '-') {
            continue;
        }

        if (opt[i][1] == '-') {
            ctx = opt[i] + 2;
        } else {
            ctx = opt[i] + 1;
        }

        if (!strcmp(ctx, "input")) {
            if (!config::input.empty()) {
                config::input += ",";
            }
            config::input += opt[++i];
        } else if (!strcmp(ctx, "output")) {
            if (!config::output.empty()) {
                config::output += ",";
            }
            config::output += opt[++i];
        } else if (!strcmp(ctx, "file")) {
            if (!config::file.empty()) {
                config::file += ",";
            }
            config::file += opt[++i];
        } else if (!strcmp(ctx, "cacheArchive")) {
            if (!config::file.empty()) {
                config::file += ",";
            }
            if (!boost::starts_with(opt[i+1], "hdfs://") || !boost::contains(opt[i+1],".tar")) {
                config::err_msg = "cacheArchive should like this hdfs://hostname:port/abc.tar.gz";
            }
            config::file += opt[++i];
        } else if (!strcmp(ctx, "mapper")) {
            if (!config::map.empty()) {
                config::map += ",";
            }
            config::map += opt[++i];
        } else if (!strcmp(ctx, "reducer")) {
            if (!config::reduce.empty()) {
                config::reduce += ",";
            }
            config::reduce += opt[++i];
        } else if (!strcmp(ctx, "partitioner")) {
            config::partitioner = ParsePartitioner(opt[++i]);
        } else if (!strcmp(ctx, "inputformat")) {
            config::input_format = ParseInputFormat(opt[++i]);
        } else if (!strcmp(ctx, "outputformat")) {
            config::output_format = ParseOutputFormat(opt[++i]);
        } else if (!strcmp(ctx, "jobconf")) {
            if (!config::jobconf.empty()) {
                config::jobconf += ",";
            }
            config::jobconf += opt[++i];
        } else if (!strcmp(ctx, "D")) {
            if (!config::jobconf.empty()) {
                config::jobconf += ",";
            }
            config::jobconf += opt[++i];
        } else if (!strcmp(ctx, "nexus")) {
            if (!config::nexus.empty()) {
                config::nexus += ",";
            }
            config::nexus += opt[++i];
        } else if (!strcmp(ctx, "nexus-file")) {
            config::nexus_file = opt[++i];
        } else if (!strcmp(ctx, "nexus-root")) {
            config::nexus_root = opt[++i];
        } else if (!strcmp(ctx, "master")) {
            config::master = opt[++i];
        } else if (!strcmp(ctx, "a") || !strcmp(ctx, "all")) {
            config::display_all = true;
            opt[i] = NULL;
            ++ ret;
            continue;
        } else if (!strcmp(ctx, "i") || !strcmp(ctx, "immediate")) {
            config::immediate_return = true;
            opt[i] = NULL;
            ++ ret;
            continue;
        } else if (!strcmp(ctx, "help") || !strcmp(ctx, "h")) {
            fprintf(stderr, "%s\n", error_message.c_str());
            exit(0);
        } else {
            continue;
        }
        opt[i] = NULL;
        opt[i - 1] = NULL;
        ++ ret;
    }

    int after = 1;
    for (int i = 1; i < cnt; ++i) {
        if (opt[i] == NULL) {
            continue;
        }
        if (i == after) {
            ++ after;
            continue;
        }
        opt[after++] = opt[i];
    }
    *argc = after;
    return ret;
}

static inline ::baidu::shuttle::sdk::JobPriority ParsePriority(const std::string& priority) {
    if (boost::iequals(priority, "veryhigh")) {
        return ::baidu::shuttle::sdk::kVeryHigh;
    } else if (boost::iequals(priority, "high")) {
        return ::baidu::shuttle::sdk::kHigh;
    } else if (boost::iequals(priority, "normal")) {
        return ::baidu::shuttle::sdk::kNormal;
    } else if (boost::iequals(priority, "low")) {
        return ::baidu::shuttle::sdk::kLow;
    }
    return ::baidu::shuttle::sdk::kUndefined;
}

static inline ::baidu::shuttle::sdk::TaskType ParseTaskType(const std::string& type) {
    if (boost::iequals(type, "map") || boost::iequals(type, "m")) {
        return ::baidu::shuttle::sdk::kMap;
    } else if (boost::iequals(type, "reduce") || boost::iequals(type, "r")) {
        return ::baidu::shuttle::sdk::kReduce;
    } else if (boost::iequals(type, "maponly") || boost::iequals(type, "map-only") ||
            boost::iequals(type, "mo")) {
        return ::baidu::shuttle::sdk::kMapOnly;
    }
    return ::baidu::shuttle::sdk::kMap;
}

static inline int64_t ParseMemory(const std::string& memory) {
    int dimension = memory.find_first_not_of("0123456789");
    int64_t base = boost::lexical_cast<int64_t>(memory.substr(0, dimension));
    for (size_t i = dimension; i < memory.size(); ++i) {
        switch (memory[i]) {
        case 'G':
        case 'g':
            base *= 1l << 30; break;
        case 'M':
        case 'm':
            base *= 1l << 20; break;
        case 'K':
        case 'k':
            base *= 1l << 10; break;
        default: return base;
        }
    }
    return base;
}

static void ParseJobConfig() {
    std::vector<std::string> opts;
    boost::split(opts, config::jobconf, boost::is_any_of(","));

    for (std::vector<std::string>::iterator it = opts.begin();
            it != opts.end(); ++it) {
        if (boost::starts_with(*it, "mapred.job.name=")) {
            config::job_name = it->substr(strlen("mapred.job.name="));
        } else if (boost::starts_with(*it, "mapred.job.priority=")) {
            config::job_priority = ParsePriority(it->substr(strlen("mapred.job.priority=")));
        } else if (boost::starts_with(*it, "mapred.job.cpu.millicores=")) {
            config::job_cpu = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.cpu.millicores=")));
        } else if (boost::starts_with(*it, "mapred.job.memory.limit=")) {
            config::job_memory = ParseMemory(it->substr(strlen("mapred.job.memory.limit=")));
        } else if (boost::starts_with(*it, "mapred.job.map.capacity=")) {
            config::map_capacity = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.map.capacity=")));
        } else if (boost::starts_with(*it, "mapred.job.reduce.capacity=")) {
            config::reduce_capacity = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.reduce.capacity=")));
        } else if (boost::starts_with(*it, "mapred.map.tasks=")) {
            config::map_tasks = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.map.tasks=")));
        } else if (boost::starts_with(*it, "mapred.reduce.tasks=")) {
            config::reduce_tasks = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.reduce.tasks=")));
        } else if (boost::starts_with(*it, "mapred.map.max.attempts=")) {
            config::map_retry = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.map.max.attempts=")));
        } else if (boost::starts_with(*it, "mapred.reduce.max.attempts=")) {
            config::reduce_retry = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.reduce.max.attempts=")));
        } else if (boost::starts_with(*it, "mapred.job.input.host=")) {
            config::input_host = it->substr(strlen("mapred.job.input.host="));
        } else if (boost::starts_with(*it, "mapred.job.input.port=")) {
            config::input_port = it->substr(strlen("mapred.job.input.port="));
        } else if (boost::starts_with(*it, "mapred.job.input.user=")) {
            config::input_user = it->substr(strlen("mapred.job.input.user="));
        } else if (boost::starts_with(*it, "mapred.job.input.password=")) {
            config::input_password = it->substr(strlen("mapred.job.input.password="));
        } else if (boost::starts_with(*it, "mapred.job.output.host=")) {
            config::output_host = it->substr(strlen("mapred.job.output.host="));
        } else if (boost::starts_with(*it, "mapred.job.output.port=")) {
            config::output_port = it->substr(strlen("mapred.job.output.port="));
        } else if (boost::starts_with(*it, "mapred.job.output.user=")) {
            config::output_user = it->substr(strlen("mapred.job.output.user="));
        } else if (boost::starts_with(*it, "mapred.job.output.password=")) {
            config::output_password = it->substr(strlen("mapred.job.output.password="));
        } else if (boost::starts_with(*it, "map.key.field.separator=")) {
            config::key_separator = it->substr(strlen("map.key.field.separator="));
        } else if (boost::starts_with(*it, "mapred.input.split.size=")) {
            config::split_size = ParseMemory(it->substr(strlen("mapred.input.split.size=")));
        } else if (boost::starts_with(*it, "stream.num.map.output.key.fields=")) {
            config::key_fields_num = boost::lexical_cast<int>(
                    it->substr(strlen("stream.num.map.output.key.fields=")));
        } else if (boost::starts_with(*it, "num.key.fields.for.partition=")) {
            config::partition_fields_num = boost::lexical_cast<int>(
                    it->substr(strlen("num.key.fields.for.partition=")));
        } else if (boost::starts_with(*it, "mapred.map.tasks.speculative.execution=")) {
            config::map_speculative_exec =
                ParseBooleanValue(it->substr(strlen("mapred.map.tasks.speculative.execution=")));
        } else if (boost::starts_with(*it, "mapred.reduce.tasks.speculative.execution=")) {
            config::reduce_speculative_exec =
                ParseBooleanValue(it->substr(strlen("mapred.reduce.tasks.speculative.execution=")));
        } else if(boost::starts_with(*it, "mapred.job.check.counters=")) {
            config::check_counters = 
                ParseBooleanValue(it->substr(strlen("mapred.job.check.counters=")));
        }
    }
}

static void ParseNexusFile() {
    if (config::nexus_file.empty()) {
        return;
    }
    std::ifstream fin(config::nexus_file.c_str());
    std::string context;
    const std::string addr_flag = "--cluster_members=";
    while (fin >> context && !fin.eof()) {
        size_t pos = context.find(addr_flag);
        if (pos == std::string::npos) {
            continue;
        }
        config::nexus = context.substr(pos + addr_flag.size());
    }
    fin.close();
}

static std::string GetMasterAddr() {
    ::galaxy::ins::sdk::SDKError err;
    ::galaxy::ins::sdk::InsSDK nexus(config::nexus);
    std::string master_path_key = config::nexus_root + config::master;
    std::string master_addr;
    bool ok = nexus.Get(master_path_key, &master_addr, &err);
    if (!ok) {
        return "";
    }
    return master_addr;
}

static inline std::string FromatLongTime(const time_t& time) {
    char time_buf[32] = { 0 };
    ::strftime(time_buf, 32, "%Y-%m-%d %H:%M:%S", ::localtime(&time));
    return time_buf;
}

static inline std::string FormatCostTime(const uint32_t& time) {
    std::stringstream ss;
    if (time > 3600) {
        ss << (time / 3600) << " hours, ";
    }
    if ((time % 3600) > 60) {
        ss << ((time % 3600) / 60) << " mins, ";
    }
    if ((time % 60) > 0) {
        ss << (time % 60) << " sec";
    }
    return ss.str();
}

static void PrintJobDetails(const ::baidu::shuttle::sdk::JobInstance& job) {
    printf("Job Name: %s\n", job.desc.name.c_str());
    printf("Job ID: %s\n", job.jobid.c_str());
    printf("User: %s\n", job.desc.user.c_str());
    printf("priority: %s\n", priority_string[job.desc.priority]);
    printf("State: %s\n", state_string[job.state]);
    printf("Start Time: %s\n", FromatLongTime(job.start_time).c_str());
    printf("Finish Time: %s\n", job.finish_time == 0 ? "-": FromatLongTime(job.finish_time).c_str());
    for (size_t i = 0; i < job.desc.inputs.size(); i++) {
        printf("Input[%d]: %s\n", (int)i, job.desc.inputs[i].c_str());
    }
    printf("Output: %s\n", job.desc.output.c_str());
    printf("Map Capacity: %d\n", job.desc.map_capacity);
    printf("Reduce Capacity: %d\n", job.desc.reduce_capacity);
    printf("\n====================\n");
    ::baidu::shuttle::TPrinter tp(7);
    tp.AddRow(7, "", "total", "pending", "running", "failed", "killed", "completed");
    tp.AddRow(7, "Map", boost::lexical_cast<std::string>(job.map_stat.total).c_str(),
              boost::lexical_cast<std::string>(job.map_stat.pending).c_str(),
              boost::lexical_cast<std::string>(job.map_stat.running).c_str(),
              boost::lexical_cast<std::string>(job.map_stat.failed).c_str(),
              boost::lexical_cast<std::string>(job.map_stat.killed).c_str(),
              boost::lexical_cast<std::string>(job.map_stat.completed).c_str());
    tp.AddRow(7, "Reduce", boost::lexical_cast<std::string>(job.reduce_stat.total).c_str(),
              boost::lexical_cast<std::string>(job.reduce_stat.pending).c_str(),
              boost::lexical_cast<std::string>(job.reduce_stat.running).c_str(),
              boost::lexical_cast<std::string>(job.reduce_stat.failed).c_str(),
              boost::lexical_cast<std::string>(job.reduce_stat.killed).c_str(),
              boost::lexical_cast<std::string>(job.reduce_stat.completed).c_str());
    printf("%s\n", tp.ToString().c_str());
}

static void PrintJobPrediction(const ::baidu::shuttle::sdk::JobInstance& job,
        const std::vector< ::baidu::shuttle::sdk::TaskInstance >& tasks) {
    if (job.map_stat.completed > 0) {
        printf("\n====================\nPrediction:\n");
        int32_t c_time_sum = 0;
        int32_t r_time_sum = 0;
        int32_t now = time(NULL);
        for (std::vector< ::baidu::shuttle::sdk::TaskInstance >::const_iterator it = tasks.begin();
                it != tasks.end(); ++it) {
            if (::baidu::shuttle::sdk::kMap == it->type ||
                    ::baidu::shuttle::sdk::kMapOnly == it->type) {
                if (::baidu::shuttle::sdk::kTaskCompleted == it->state) {
                    c_time_sum += it->end_time - it->start_time;
                } else if (::baidu::shuttle::sdk::kTaskRunning == it->state) {
                    r_time_sum += now - it->start_time;
                }
            }
        }
        int32_t avg_time = c_time_sum / job.map_stat.completed;
        printf("Average map time by completed map : %s\n", 
                FormatCostTime(avg_time).c_str());
        int32_t all_need_time = avg_time * job.map_stat.total;
        int32_t more_need_time = all_need_time - c_time_sum - r_time_sum;
        if (more_need_time > 0) {
            float success_rate = (float)job.map_stat.completed / 
                (job.map_stat.completed + job.map_stat.killed + job.map_stat.failed);
            int more_cost_time = int(more_need_time / success_rate /
                (job.map_stat.running ? job.map_stat.running : 1));
            printf("need more %s to complete map, ", FormatCostTime(more_cost_time).c_str());
            printf("map may complete @ %s\n", FromatLongTime(now + more_cost_time).c_str());
        }
        printf("\n====================\n");
    }
}

static void PrintTasksInfo(const std::vector< ::baidu::shuttle::sdk::TaskInstance >& tasks) {
    const int column = 6;
    ::baidu::shuttle::TPrinter tp(column);
    tp.AddRow(column, "tid", "aid", "state", "minion address", "start time", "end time");
    for (std::vector< ::baidu::shuttle::sdk::TaskInstance >::const_iterator it = tasks.begin();
            it != tasks.end(); ++it) {
        tp.AddRow(column, (task_type_string[it->type] + "-" +
                      boost::lexical_cast<std::string>(it->task_id)).c_str(),
                  boost::lexical_cast<std::string>(it->attempt_id).c_str(),
                  (it->state == ::baidu::shuttle::sdk::kTaskUnknown) ?
                      "Unknown" : state_string[it->state],
                  it->minion_addr.c_str(),
                  FromatLongTime(it->start_time).c_str(),
                  (it->end_time > it->start_time) ? FromatLongTime(it->end_time).c_str() : "-");
    }
    printf("%s\n", tp.ToString().c_str());
}

static void PrintJobsInfo(std::vector< ::baidu::shuttle::sdk::JobInstance >& jobs) {
    const int column = 5;
    ::baidu::shuttle::TPrinter tp(column);
    tp.SetMaxColWidth(80);
    tp.AddRow(column, "job id", "job name", "state", "map(r/p/c)", "reduce(r/p/c)");
    std::sort(jobs.begin(), jobs.end(), JobComparator());
    for (std::vector< ::baidu::shuttle::sdk::JobInstance >::const_iterator it = jobs.begin();
            it != jobs.end(); ++it) {
        std::string map_running = boost::lexical_cast<std::string>(it->map_stat.running)
            + "/" + boost::lexical_cast<std::string>(it->map_stat.pending)
            + "/" + boost::lexical_cast<std::string>(it->map_stat.completed);
        std::string reduce_running = boost::lexical_cast<std::string>(it->reduce_stat.running)
            + "/" + boost::lexical_cast<std::string>(it->reduce_stat.pending)
            + "/" + boost::lexical_cast<std::string>(it->reduce_stat.completed);
        tp.AddRow(column, it->jobid.c_str(), it->desc.name.c_str(), state_string[it->state],
                  map_running.c_str(), reduce_running.c_str());
    }
    printf("%s\n", tp.ToString().c_str());
}

static int MonitorJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::params.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);
    done = true;
    char erase[85] = { 0 };
    memset(erase, ' ', 84);
    bool is_tty = ::isatty(fileno(stdout));
    int error_tolerance = 5;
    int monitor_interval = 2;
    if (!is_tty) {
        monitor_interval = 20;
    }
    while (true) {
        ::baidu::shuttle::sdk::JobInstance job;
        std::vector< ::baidu::shuttle::sdk::TaskInstance > tasks;
        const std::string timestamp = FromatLongTime(time(NULL));
        std::string error_msg;
        std::map<std::string, int64_t> counters;
        bool ok = shuttle->ShowJob(config::params[0], job, tasks, 
                                   true, false, error_msg, counters);
        if (!ok) {
            fprintf(stderr, "lost connection with master\n");
            if (error_tolerance-- <= 0) {
                break;
            }
            sleep(5);
            continue;
        }
        switch (job.state) {
        case ::baidu::shuttle::sdk::kPending:
            if (is_tty) {
                printf("\r[%s] job pending...", timestamp.c_str());
            } else {
                printf("[%s] job pending...\n", timestamp.c_str());
            }
            fflush(stdout);
            break;
        case ::baidu::shuttle::sdk::kRunning:
            if (is_tty) {
                printf("\r%s", erase);
                printf("\r[%s] job is running, map: %d/%d, %d running; reduce: %d/%d, %d running",
                        timestamp.c_str(),
                        job.map_stat.completed, job.map_stat.total, job.map_stat.running,
                        job.reduce_stat.completed, job.reduce_stat.total, job.reduce_stat.running);
            } else {
                printf("[%s] job is running, map: %d/%d, %d running; reduce: %d/%d, %d running\n",
                        timestamp.c_str(),
                        job.map_stat.completed, job.map_stat.total, job.map_stat.running,
                        job.reduce_stat.completed, job.reduce_stat.total, job.reduce_stat.running);
            }
            fflush(stdout);
            break;
        case ::baidu::shuttle::sdk::kCompleted:
            if (is_tty) {
                printf("\r%s", erase);
                printf("\r[%s] job is running, map: %d/%d, %d running; reduce: %d/%d, %d running\n",
                        timestamp.c_str(),
                        job.map_stat.completed, job.map_stat.total, job.map_stat.running,
                        job.reduce_stat.completed, job.reduce_stat.total, job.reduce_stat.running);
            } else {
                printf("[%s] job is running, map: %d/%d, %d running; reduce: %d/%d, %d running\n",
                        timestamp.c_str(),
                        job.map_stat.completed, job.map_stat.total, job.map_stat.running,
                        job.reduce_stat.completed, job.reduce_stat.total, job.reduce_stat.running);
            }
            fflush(stdout);
            printf("[%s] job `%s' has completed\n", timestamp.c_str(), job.desc.name.c_str());
            return 0;
        case ::baidu::shuttle::sdk::kFailed:
            if (is_tty) {
                fprintf(stderr, "\n[%s] job `%s' is failed\n", timestamp.c_str(), job.desc.name.c_str());
            } else {
                fprintf(stderr, "[%s] job `%s' is failed\n", timestamp.c_str(), job.desc.name.c_str());
            }
            fprintf(stderr, "=== error msg ===\n");
            fprintf(stderr, "%s", error_msg.c_str());
            fprintf(stderr, "==================\n");
            fprintf(stderr, "check error details here: %s\n", (job.desc.output + "/_temporary/errors").c_str());
            fflush(stderr);
            delete shuttle;
            return -1;
        case ::baidu::shuttle::sdk::kKilled:
            if (is_tty) {
                fprintf(stderr, "\n[%s] job `%s' is killed\n", timestamp.c_str(), job.desc.name.c_str());
            } else {
                fprintf(stderr, "[%s] job `%s' is killed\n", timestamp.c_str(), job.desc.name.c_str());
            }
            fflush(stderr);
            delete shuttle;
            return -1;
        }
        sleep(monitor_interval);
    }
    delete shuttle;
    if (error_tolerance <= 0) {
        return 1;
    }
    return 0;
}

static int SubmitJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }

    if (config::file.empty()) {
        fprintf(stderr, "file flag is needed, use --file to specify\n");
        return -1;
    }
    if (config::input.empty()) {
        fprintf(stderr, "input flag is needed, use --input to specify\n");
        return -1;
    }
    if (config::output.empty()) {
        fprintf(stderr, "output flag is needed, use --output to specify\n");
        return -1;
    }
    if (config::map.empty()) {
        fprintf(stderr, "map flag is needed, use --map to specify\n");
        return -1;
    }
    if (config::reduce_tasks != 0 && config::reduce.empty()) {
        fprintf(stderr, "reduce flag is needed, use --reduce to specify\n");
        return -1;
    }
/*  if (config::input_host.empty() || config::input_port.empty() ||
            config::input_user.empty() || config::input_password.empty()) {
        fprintf(stderr, "input dfs info is needed, use --jobconf to specify\n");
        return -1;
    }
    if (config::output_host.empty() || config::output_port.empty() ||
            config::output_user.empty() || config::output_password.empty()) {
        fprintf(stderr, "output dfs info is needed, use --jobconf to specify\n");
        return -1;
    }*/
    if (config::map_capacity == -1) {
        config::map_capacity = 100;
    }
    if (config::reduce_capacity == -1) {
        config::reduce_capacity = 100;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);
    ::baidu::shuttle::sdk::JobDescription job_desc;
    job_desc.name = config::job_name;
    job_desc.priority = config::job_priority;
    job_desc.map_capacity = config::map_capacity;
    job_desc.reduce_capacity = config::reduce_capacity;
    job_desc.millicores = config::job_cpu;
    job_desc.memory = config::job_memory;
    boost::split(job_desc.files, config::file, boost::is_any_of(","));
    boost::split(job_desc.inputs, config::input, boost::is_any_of(","));
    job_desc.output = config::output;
    job_desc.map_command = config::map;
    job_desc.reduce_command = config::reduce;
    job_desc.partition = config::partitioner;
    job_desc.map_total = config::map_tasks;
    job_desc.reduce_total = config::reduce_tasks;
    job_desc.key_separator = config::key_separator;
    job_desc.key_fields_num = config::key_fields_num;
    job_desc.partition_fields_num = config::partition_fields_num;
    job_desc.input_dfs.host = config::input_host;
    job_desc.input_dfs.port = config::input_port;
    job_desc.input_dfs.user = config::input_user;
    job_desc.input_dfs.password = config::input_password;
    job_desc.output_dfs.host = config::output_host;
    job_desc.output_dfs.port = config::output_port;
    job_desc.output_dfs.user = config::output_user;
    job_desc.output_dfs.password = config::output_password;
    job_desc.input_format = config::input_format;
    job_desc.output_format = config::output_format;
    job_desc.pipe_style = config::pipe_style;
    job_desc.map_allow_duplicates = config::map_speculative_exec;
    job_desc.check_counters = config::check_counters;
    job_desc.reduce_allow_duplicates = config::reduce_speculative_exec;
    job_desc.map_retry = config::map_retry;
    job_desc.reduce_retry = config::reduce_retry;
    job_desc.split_size = config::split_size;

    std::string jobid;
    bool ok = shuttle->SubmitJob(job_desc, jobid);
    delete shuttle;
    done = true;
    if (!ok) {
        fprintf(stderr, "submit job failed\n");
        return 1;
    }
    printf("job successfully submitted, id: %s\n", jobid.c_str());
    if (config::immediate_return) {
        return 0;
    }
    config::params.clear();
    config::params.push_back(jobid);
    return MonitorJob();
}

static int UpdateJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::params.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    bool ok = shuttle->UpdateJob(config::params[0], config::job_priority,
                                 config::map_capacity, config::reduce_capacity);
    delete shuttle;
    done = true;
    if (!ok) {
        fprintf(stderr, "update job failed\n");
        return 1;
    }
    return 0;
}

static inline void ParseTaskNumber(const std::string& description,
                                   ::baidu::shuttle::sdk::TaskType& mode,
                                   int& task_id, int& attempt_id) {
    std::vector<std::string> parts;
    boost::split(parts, description, boost::is_any_of("-"));
    if (parts.size() != 3) {
        return;
    }
    mode = ParseTaskType(parts[0]);
    task_id = boost::lexical_cast<int>(parts[1]);
    attempt_id = boost::lexical_cast<int>(parts[2]);
}

static int Kill() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::params.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    bool ok = true;
    if (config::params.size() == 1) {
        ok = shuttle->KillJob(config::params[0]);
    } else {
        ::baidu::shuttle::sdk::TaskType mode = ::baidu::shuttle::sdk::kMap;
        int task_id = -1, attempt = 0;
        ParseTaskNumber(config::params[1], mode, task_id, attempt);
        ok = shuttle->KillTask(config::params[0], mode, task_id, attempt);
    }
    delete shuttle;
    done = true;
    if (!ok) {
        fprintf(stderr, "kill failed\n");
        return 1;
    }
    return 0;
}

static int ListJobs() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    std::vector< ::baidu::shuttle::sdk::JobInstance > jobs;
    bool ok = shuttle->ListJobs(jobs, config::display_all);
    delete shuttle;
    done = true;
    if (!ok) {
        fprintf(stderr, "list job failed\n");
        return 1;
    }
    PrintJobsInfo(jobs);
    return 0;
}

static void PrintJobCounters(const std::map<std::string, int64_t>& counters) {
    if (counters.size() == 0) {
        return;
    }
    ::baidu::shuttle::TPrinter tp(2);
    tp.SetMaxColWidth(80);
    printf("\n");
    tp.AddRow(2, "Counter-Name", "Value");
    std::map<std::string, int64_t>::const_iterator it;
    for (it = counters.begin(); it != counters.end(); it++) {
        tp.AddRow(2, it->first.c_str(),
                  boost::lexical_cast<std::string>(it->second).c_str());
    }
    printf("%s\n", tp.ToString().c_str());
}

static int ShowJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::params.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    ::baidu::shuttle::sdk::JobInstance job;
    std::vector< ::baidu::shuttle::sdk::TaskInstance > tasks;
    std::string error_msg;
    std::map<std::string, int64_t> counters;
    bool ok = shuttle->ShowJob(config::params[0], job, tasks, 
                               config::display_all, true, error_msg, counters);
    delete shuttle;
    done = true;
    if (!ok) {
        fprintf(stderr, "show job status failed\n");
        return 1;
    }
    std::sort(tasks.begin(), tasks.end(), TaskComparator());
    PrintJobDetails(job);
    PrintJobPrediction(job, tasks);
    PrintJobCounters(counters);
    PrintTasksInfo(tasks);
    if (!error_msg.empty()) {
        printf("===== error message =====\n");
        printf("%s", error_msg.c_str());
        printf("=========================\n");
    }
    return 0;
}

void* LongPeriodWarning(void* /*args*/) {
    sleep(5);
    if (!done) {
        fprintf(stderr, "Seems the input scale is quite large.\n");
        fprintf(stderr, "  please wait for a while...\n");
    }
    pthread_exit(NULL);
}

void SignalHandler(int /*sig*/) {
    if (done) {
        printf("\nyour job is running, you can use `status' or `list' command to check it\n");
        exit(0);
    }
    exit(-1);
}

int main(int argc, char* argv[]) {
    ParseCommandLineFlags(&argc, &argv);
    if (!config::err_msg.empty()) {
        fprintf(stderr, "%s\n", config::err_msg.c_str());
        return -1;
    }
    ParseJobConfig();
    ParseNexusFile();
    if (argc < 2) {
        fprintf(stderr, "%s", error_message.c_str());
        return -1;
    }
    if (argc > 2) {
        for (int i = 2; i < argc; ++i) {
            if (argv[i][0] == '-') {
                continue;
            }
            config::params.push_back(argv[i]);
        }
    }

    pthread_t tid;
    pthread_create(&tid, NULL, LongPeriodWarning, NULL);
    if (!strcmp(argv[1], "streaming")) {
        config::pipe_style = ::baidu::shuttle::sdk::kStreaming;
        signal(SIGINT, SignalHandler);
        return SubmitJob();
    } else if (!strcmp(argv[1], "bistreaming")) {
        config::pipe_style = ::baidu::shuttle::sdk::kBiStreaming;
        signal(SIGINT, SignalHandler);
        return SubmitJob();
    } else if (!strcmp(argv[1], "update")) {
        return UpdateJob();
    } else if (!strcmp(argv[1], "kill")) {
        return Kill();
    } else if (!strcmp(argv[1], "list")) {
        return ListJobs();
    } else if (!strcmp(argv[1], "status")) {
        return ShowJob();
    } else if (!strcmp(argv[1], "monitor")) {
        return MonitorJob();
    } else {
        fprintf(stderr, "unknown op: %s\n", argv[1]);
        fprintf(stderr, "  use -h/--help for more introduction\n");
    }
    return 0;
}

