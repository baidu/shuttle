#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <fstream>

#include <cstdio>
#include <cstring>
#include <tprinter.h>

#include "sdk/shuttle.h"
#include "ins_sdk.h"
#include "tprinter.h"

namespace config {

std::string param;

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

std::string job_name = "map_reduce_job";
::baidu::shuttle::sdk::JobPriority job_priority = \
    ::baidu::shuttle::sdk::kUndefined;
::baidu::shuttle::sdk::PartitionMethod partitioner = \
    ::baidu::shuttle::sdk::kKeyFieldBased;
::baidu::shuttle::sdk::InputFormat input_format = \
    ::baidu::shuttle::sdk::kTextInput;
::baidu::shuttle::sdk::OutputFormat output_format = \
    ::baidu::shuttle::sdk::kTextOutput;
int job_cpu = 1000;
int64_t job_memory = 1024 * 1024 * 1024;
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

}

const std::string error_message = "shuttle client - A fast computing framework base on Galaxy\n"
        "Usage:\n"
        "\tshuttle submit [flags]\n"
        "\tshuttle update <jobid> [new flags]\n"
        "\tshuttle kill <jobid>\n"
        "\tshuttle list\n"
        "\tshuttle status <jobid>\n"
        "Options:\n"
        "\t-h  --help\t\t\tShow this information\n"
        "\t-a  --all\t\t\tConsider finished and dead jobs in status and list operation\n"
        "\t-input <file>\t\t\tSpecify the input file, using a hdfs path\n"
        "\t-output <path>\t\t\tSpecify the output path, which must be empty\n"
        "\t-file <file>[,...]\t\tSpecify the files needed by your program\n"
        "\t-map <file>\t\t\tSpecify the map program\n"
        "\t-reduce <file>\t\t\tSpecify the reduce program\n"
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
        "\t  mapred.job.map.tasks\t\tSpecify the number of map tasks\n"
        "\t  mapred.job.reduce.tasks\tSpecify the number of reduce tasks\n"
        "\t  mapred.job.input.host\t\tSpecify the host of the dfs which stores inputs\n"
        "\t  mapred.job.input.port\t\tSpecify the port, ditto\n"
        "\t  mapred.job.input.user\t\tSpecify the user, ditto\n"
        "\t  mapred.job.input.password\tSpecify the password, ditto\n"
        "\t  mapred.job.output.host\tSpecify the host of the dfs which stores output\n"
        "\t  mapred.job.output.port\tSpecify the port, ditto\n"
        "\t  mapred.job.output.user\tSpecify the user, ditto\n"
        "\t  mapred.job.output.password\tSpecify the password, ditto\n"
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
    }
    return ::baidu::shuttle::sdk::kTextOutput;
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
        } else if (!strcmp(ctx, "map")) {
            if (!config::map.empty()) {
                config::map += ",";
            }
            config::map += opt[++i];
        } else if (!strcmp(ctx, "reduce")) {
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
            config::job_memory = boost::lexical_cast<int64_t>(
                    it->substr(strlen("mapred.job.memory.limit=")));
        } else if (boost::starts_with(*it, "mapred.job.map.capacity=")) {
            config::map_capacity = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.map.capacity=")));
        } else if (boost::starts_with(*it, "mapred.job.reduce.capacity=")) {
            config::reduce_capacity = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.reduce.capacity=")));
        } else if (boost::starts_with(*it, "mapred.job.map.tasks=")) {
            config::map_tasks = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.map.tasks=")));
        } else if (boost::starts_with(*it, "mapred.job.reduce.tasks=")) {
            config::reduce_tasks = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.reduce.tasks=")));
        } else if (boost::starts_with(*it, "mapred.job.input.host=")) {
            config::input_host = it->substr(strlen("mapred.job.input.host="));
        } else if (boost::starts_with(*it, "mapred.job.input.port=")) {
            config::input_host = it->substr(strlen("mapred.job.input.port="));
        } else if (boost::starts_with(*it, "mapred.job.input.user=")) {
            config::input_host = it->substr(strlen("mapred.job.input.user="));
        } else if (boost::starts_with(*it, "mapred.job.input.password=")) {
            config::input_host = it->substr(strlen("mapred.job.input.password="));
        } else if (boost::starts_with(*it, "mapred.job.output.host=")) {
            config::output_host = it->substr(strlen("mapred.job.output.host="));
        } else if (boost::starts_with(*it, "mapred.job.output.port=")) {
            config::output_host = it->substr(strlen("mapred.job.output.port="));
        } else if (boost::starts_with(*it, "mapred.job.output.user=")) {
            config::output_host = it->substr(strlen("mapred.job.output.user="));
        } else if (boost::starts_with(*it, "mapred.job.output.password=")) {
            config::output_host = it->substr(strlen("mapred.job.output.password="));
        } else if (boost::starts_with(*it, "map.key.field.separator=")) {
            config::key_separator = it->substr(strlen("map.key.field.separator="));
        } else if (boost::starts_with(*it, "stream.num.map.output.key.fields=")) {
            config::key_fields_num = boost::lexical_cast<int>(
                    it->substr(strlen("stream.num.map.output.key.fields=")));
        } else if (boost::starts_with(*it, "num.key.fields.for.partition=")) {
            config::partition_fields_num = boost::lexical_cast<int>(
                    it->substr(strlen("num.key.fields.for.partition=")));
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

static void PrintJobDetails(const ::baidu::shuttle::sdk::JobInstance& job) {
    printf("Job Name: %s\n", job.desc.name.c_str());
    printf("Job ID: %s\n", job.jobid.c_str());
    printf("User: %s\n", job.desc.user.c_str());
    printf("priority: %s\n", priority_string[job.desc.priority]);
    printf("State: %s\n", state_string[job.state]);
    printf("====================\n");
    ::baidu::common::TPrinter tp(7);
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

static void PrintTasksInfo(const std::vector< ::baidu::shuttle::sdk::TaskInstance >& tasks) {
    ::baidu::common::TPrinter tp(5);
    tp.AddRow(5, "tid", "aid", "state", "minion address", "progress");
    for (std::vector< ::baidu::shuttle::sdk::TaskInstance >::const_iterator it = tasks.begin();
            it != tasks.end(); ++it) {
        tp.AddRow(5, (task_type_string[it->type] + "-" +
                      boost::lexical_cast<std::string>(it->task_id)).c_str(),
                  boost::lexical_cast<std::string>(it->attempt_id).c_str(),
                  (it->state == ::baidu::shuttle::sdk::kTaskUnknown) ?
                      "Unknown" : state_string[it->state],
                  it->minion_addr.c_str(),
                  boost::lexical_cast<std::string>(it->progress).c_str());
    }
    printf("%s\n", tp.ToString().c_str());
}

static void PrintJobsInfo(const std::vector< ::baidu::shuttle::sdk::JobInstance >& jobs) {
    ::baidu::common::TPrinter tp(4);
    tp.AddRow(4, "job id", "state", "map(r/p/c)", "reduce(r/p/c)");
    for (std::vector< ::baidu::shuttle::sdk::JobInstance >::const_iterator it = jobs.begin();
            it != jobs.end(); ++it) {
        std::string map_running = boost::lexical_cast<std::string>(it->map_stat.running)
            + "/" + boost::lexical_cast<std::string>(it->map_stat.pending)
            + "/" + boost::lexical_cast<std::string>(it->map_stat.completed);
        std::string reduce_running = boost::lexical_cast<std::string>(it->reduce_stat.running)
            + "/" + boost::lexical_cast<std::string>(it->reduce_stat.pending)
            + "/" + boost::lexical_cast<std::string>(it->reduce_stat.completed);
        tp.AddRow(4, it->jobid.c_str(), state_string[it->state],
                  map_running.c_str(), reduce_running.c_str());
    }
    printf("%s\n", tp.ToString().c_str());
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
        config::reduce_capacity = 10;
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

    std::string jobid;
    bool ok = shuttle->SubmitJob(job_desc, jobid);
    if (!ok) {
        fprintf(stderr, "submit job failed\n");
        return 1;
    }
    printf("job successfully submitted, id: %s\n", jobid.c_str());
    return 0;
}

static int UpdateJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::param.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    if (config::param[0] == '-') {
        fprintf(stderr, "invalid flag detected\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    bool ok = shuttle->UpdateJob(config::param, config::job_priority,
                                 config::map_capacity, config::reduce_capacity);
    if (!ok) {
        fprintf(stderr, "update job failed\n");
        return 1;
    }
    return 0;
}

static int KillJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::param.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    if (config::param[0] == '-') {
        fprintf(stderr, "invalid flag detected\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    bool ok = shuttle->KillJob(config::param);
    if (!ok) {
        fprintf(stderr, "kill job failed\n");
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
    if (!ok) {
        fprintf(stderr, "list job failed\n");
        return 1;
    }
    PrintJobsInfo(jobs);
    return 0;
}

static int ShowJob() {
    std::string master_endpoint = GetMasterAddr();
    if (master_endpoint.empty()) {
        fprintf(stderr, "fail to get master endpoint\n");
        return -1;
    }
    if (config::param.empty()) {
        fprintf(stderr, "job id is required\n");
        return -1;
    }
    if (config::param[0] == '-') {
        fprintf(stderr, "invalid flag detected\n");
        return -1;
    }
    ::baidu::shuttle::Shuttle *shuttle = ::baidu::shuttle::Shuttle::Connect(master_endpoint);

    ::baidu::shuttle::sdk::JobInstance job;
    std::vector< ::baidu::shuttle::sdk::TaskInstance > tasks;
    bool ok = shuttle->ShowJob(config::param, job, tasks, config::display_all);
    if (!ok) {
        fprintf(stderr, "show job status failed\n");
        return 1;
    }
    PrintJobDetails(job);
    PrintTasksInfo(tasks);
    return 0;
}

int main(int argc, char* argv[]) {
    ParseCommandLineFlags(&argc, &argv);
    ParseJobConfig();
    ParseNexusFile();
    if (argc < 2) {
        fprintf(stderr, "%s", error_message.c_str());
        return -1;
    }
    if (argc > 2) {
        config::param = argv[2];
    }

    if (!strcmp(argv[1], "submit")) {
        return SubmitJob();
    } else if (!strcmp(argv[1], "update")) {
        return UpdateJob();
    } else if (!strcmp(argv[1], "kill")) {
        return KillJob();
    } else if (!strcmp(argv[1], "list")) {
        return ListJobs();
    } else if (!strcmp(argv[1], "status")) {
        return ShowJob();
    }
    return 0;
}

