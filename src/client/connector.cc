#include "connector.h"

#include <boost/lexical_cast.hpp>
#include <unistd.h>
#include "sdk/shuttle.h"
#include "client/config.h"
#include "ins_sdk.h"

namespace baidu {
namespace shuttle {

ShuttleConnector::ShuttleConnector(Configuration* config)
        : config_(config) {
    if (config == NULL) {
        return;
    }
    const std::string& master = GetMasterAddr();
    if (master.empty()) {
        return;
    }
    sdk_ = Shuttle::Connect(master);
}

int ShuttleConnector::Submit() {
    sdk::JobDescription job;
    int ret = config_->BuildJobDescription(job);
    if (ret != 0) {
        return ret;
    }
    std::string job_id;
    bool ok = sdk_->SubmitJob(job, job_id);
    if (!ok) {
        std::cerr << "ERROR: failed to submit job" << std::endl;
        return -1;
    }
    std::cout << "INFO: job successfully submitted, id = " << job_id << std::endl;
    if (config_->GetConf("i") == "true") {
        return 0;
    }
    return Monitor();
}

int ShuttleConnector::Update() {
    std::vector<std::string> subcommands;
    config_->GetConf("subcommand", subcommands);
    if (subcommands.size() < 3) {
        std::cerr << "ERROR: setting capacity needs more parameters" << std::endl;
        return -1;
    }
    int node = -1, capacity = -1;
    try {
        node = boost::lexical_cast<int>(subcommands[1]);
        capacity = boost::lexical_cast<int>(subcommands[2]);
    } catch (const boost::bad_lexical_cast&) {
        std::cerr << "ERROR: bad update parameters" << std::endl;
        return -1;
    }
    std::map<int32_t, int32_t> new_capacity;
    new_capacity[node] = capacity;
    bool ok = sdk_->UpdateJob(subcommands[0], new_capacity);
    if (!ok) {
        std::cerr << "ERROR: failed to set new capacity" << std::endl;
        return -1;
    }
    return 0;
}

int ShuttleConnector::Kill() {
    std::vector<std::string> subcommands;
    config_->GetConf("subcommand", subcommands);
    if (subcommands.empty()) {
        std::cerr << "ERROR: please provide job id to kill" << std::endl;
        return -1;
    }
    const std::string& job = subcommands[0];
    bool ok = true;
    if (subcommands.size() == 1) {
        // kill whole job
        ok = sdk_->KillJob(job);
    } else {
        // kill a certain attempt
        int node = -1, tid = -1, aid = -1;
        const std::string& task = subcommands[1];
        try {
            size_t sep1 = task.find_first_of("-");
            if (sep1 == std::string::npos) {
                std::cerr << "ERROR: invalid task format, "
                          << "should be \%d-\%d-\%d" << std::endl;
                return -1;
            }
            node = boost::lexical_cast<int>(task.substr(0, sep1));
            size_t sep2 = task.find_first_of("-", sep1 + 1);
            if (sep2 == std::string::npos) {
                std::cerr << "ERROR: invalid task format, "
                          << "should be \%d-\%d-\%d" << std::endl;
                return -1;
            }
            tid = boost::lexical_cast<int>(task.substr(sep1 + 1, sep2 - sep1 - 1));
            aid = boost::lexical_cast<int>(task.substr(sep2 + 1));
        } catch (const boost::bad_lexical_cast&) {
            std::cerr << "ERROR: invalid task format, "
                      << "should be \%d-\%d-\%d" << std::endl;
            return -1;
        }
        ok = sdk_->KillTask(job, node, tid, aid);
    }
    if (!ok) {
        std::cerr << "ERROR: failed to kill job" << std::endl;
        return -1;
    }
    return 0;
}

int ShuttleConnector::List() {
    return -1;
}

int ShuttleConnector::Status() {
    return -1;
}

int ShuttleConnector::Monitor() {
    std::vector<std::string> subcommands;
    config_->GetConf("subcommand", subcommands);
    if (subcommands.empty()) {
        std::cerr << "ERROR: please provide job id to monitor" << std::endl;
        return -1;
    }
    bool is_tty = ::isatty(fileno(stdout));
    int interval = is_tty ? 2 : 20;
    int error_tolerance = 5;
    while (true) {
        sdk::JobInstance job;
        std::vector<sdk::TaskInstance> tasks;
        if (!sdk_->ShowJob(subcommands[0], job, tasks, true)) {
            std::cerr << "ERROR: failed to show job status" << std::endl;
            if (error_tolerance-- <= 0) {
                break;
            }
            sleep(5);
            continue;
        }
        switch (job.state) {
        case sdk::kPending:
            if (is_tty) {
                std::cout << "\x1B[2K\x1B[0E"
                    << "[" << Timestamp() << "] job is pending...";
            } else {
                std::cout << "[" << Timestamp() << "] job is pending..." << std::endl;
            }
            std::flush(std::cout);
            break;
        case sdk::kRunning:
        case sdk::kCompleted:
            return 0;
        case sdk::kFailed:
            return -1;
        case sdk::kKilled:
            return -1;
        }
        sleep(interval);
    }
    return -1;
}

std::string ShuttleConnector::GetMasterAddr() {
    galaxy::ins::sdk::SDKError error;
    galaxy::ins::sdk::InsSDK nexus(config_->GetConf("nexus"));
    const std::string& master_path = config_->GetConf("nexus-root")
        + config_->GetConf("master");
    std::string master_addr;
    bool ok = nexus.Get(master_path, &master_addr, &error);
    return ok ? master_addr : "";
}

}
}

