#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include <cstdio>
#include <cstring>
#include <tprinter.h>

#include "sdk/shuttle.h"
#include "ins_sdk.h"

namespace config {

std::string input;
std::string output;
std::string file;
std::string map;
std::string reduce;
std::string jobconf;
std::string job_name = "map_reduce_job";
std::string job_priority;
int job_cpu = 10;
int job_memory = 1000;
int map_capacity = 10;
int reduce_capacity = 10;
int map_tasks = 5000;
int reduce_tasks = 1;

}

const std::string error_message = "shuttle client - A fast computing framework base on Galaxy\n"
        "Usage:\n"
        "\tshuttle submit <jobfile> [flags]\n"
        "\tshuttle update <jobid> [new flags]\n"
        "\tshuttle kill <jobid>\n"
        "\tshuttle list\n"
        "\tshuttle status <jobid>\n"
        "Options:\n"
        "\t-input <file>\t\t\tSpecify the input file, using a hdfs path\n"
        "\t-output <path>\t\t\tSpecify the output path, which must be empty\n"
        "\t-file <file>[,...]\t\tSpecify the files needed by your program\n"
        "\t-map <file>\t\t\tSpecify the map program\n"
        "\t-reduce <file>\t\t\tSpecify the reduce program\n"
        "\t-jobconf <key>=<value>[,...]\tSpecify the configuration of the job\n"
        "\t  mapred.job.name\t\tName the submitting job\n"
        "\t  mapred.job.priority\t\tSpecify the priority of the job\n"
        "\t  mapred.job.cpu.millicores\tSpecify the cpu resource occuptation\n"
        "\t  mapred.job.memory.limit\tSpecify the memory resource occuptation\n"
        "\t  mapred.job.map.capacity\tSpecify the slot number that map tasks can use\n"
        "\t  mapred.job.reduce.capacity\tSpecify the slot number that reduce tasks can use\n"
        "\t  mapred.job.map.tasks\t\tSpecify the number of map tasks\n"
        "\t  mapred.job.reduce.tasks\tSpecify the number of reduce tasks\n"
;

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
        } else if (!strcmp(ctx, "jobconf")) {
            if (!config::jobconf.empty()) {
                config::jobconf += ",";
            }
            config::jobconf += opt[++i];
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

static void ParseJobConfig() {
    std::vector<std::string> opts;
    boost::split(opts, config::jobconf, boost::is_any_of(","));

    for (std::vector<std::string>::iterator it = opts.begin();
            it != opts.end(); ++it) {
        if (boost::starts_with(*it, "mapred.job.name=")) {
            config::job_name = it->substr(strlen("mapred.job.name="));
        } else if (boost::starts_with(*it, "mapred.job.priority=")) {
            config::job_priority = it->substr(strlen("mapred.job.priority="));
        } else if (boost::starts_with(*it, "mapred.job.cpu.millicores=")) {
            config::job_cpu = boost::lexical_cast<int>(
                    it->substr(strlen("mapred.job.cpu.millicores=")));
        } else if (boost::starts_with(*it, "mapred.job.memory.limit=")) {
            config::job_memory = boost::lexical_cast<int>(
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
        }
    }
}

static int SubmitJob() {
    return 0;
}

static int UpdateJob() {
    return 0;
}

static int KillJob() {
    return 0;
}

static int ListJobs() {
    return 0;
}

static int ShowJob() {
    return 0;
}

int main(int argc, char* argv[]) {
    ParseCommandLineFlags(&argc, &argv);
    ParseJobConfig();
    if (argc < 2) {
        fprintf(stderr, "%s", error_message.c_str());
        return -1;
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

