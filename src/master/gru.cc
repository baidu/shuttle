#include "gru.h"

#include <sstream>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>
#include <vector>
#include "logging.h"

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_string(nexus_server_list);
DECLARE_string(nexus_root_path);
DECLARE_string(master_path);
DECLARE_bool(enable_cpu_soft_limit);

namespace baidu {
namespace shuttle {

static const int64_t default_additional_map_memory = 1024l * 1024 * 1024;
static const int64_t default_additional_reduce_memory = 2048l * 1024 * 1024;
static const int default_map_additional_millicores = 0;
static const int default_reduce_additional_millicores = 500;

int Gru::additional_map_millicores = default_map_additional_millicores;
int Gru::additional_reduce_millicores = default_reduce_additional_millicores;
int64_t Gru::additional_map_memory = default_additional_map_memory;
int64_t Gru::additional_reduce_memory = default_additional_reduce_memory;

Gru::Gru(::baidu::galaxy::Galaxy* galaxy, JobDescriptor* job,
         const std::string& job_id, WorkMode mode) :
        galaxy_(galaxy), job_(job), job_id_(job_id), mode_(mode) {
    mode_str_ = ((mode == kReduce) ? "reduce" : "map");
    minion_name_ = job->name() + "_" + mode_str_;
}

Status Gru::Start() {
    ::baidu::galaxy::JobDescription galaxy_job;
    galaxy_job.job_name = minion_name_ + "@minion";
    galaxy_job.type = "kLongRun";
    galaxy_job.priority = "kOnline";
    galaxy_job.replica = (mode_ == kReduce) ? job_->reduce_capacity() : job_->map_capacity();
    galaxy_job.deploy_step = FLAGS_galaxy_deploy_step;
    galaxy_job.pod.version = "1.0.0";
    if (mode_str_ == "map") {
        galaxy_job.pod.requirement.millicores = job_->millicores() + additional_map_millicores;
    } else {
        galaxy_job.pod.requirement.millicores = job_->millicores() + additional_reduce_millicores;
    }
    galaxy_job.pod.requirement.memory = job_->memory() +
        ((mode_ == kReduce) ? additional_reduce_memory : additional_map_memory);
    std::string app_package;
    std::vector<std::string> cache_archive_list;
    int file_size = job_->files().size();
    for (int i = 0; i < file_size; ++i) {
        const std::string& file = job_->files(i);
        if (boost::starts_with(file, "hdfs://")) {
            cache_archive_list.push_back(file);
        } else {
            app_package = file;
        }
    }
    std::stringstream ss;
    for (size_t i = 0; i < cache_archive_list.size(); i++) {
        ss << "cache_archive_" << i << "=" << cache_archive_list[i] << " ";
    }
    ss << "app_package=" << app_package
       << " ./minion_boot.sh -jobid=" << job_id_ << " -nexus_addr=" << FLAGS_nexus_server_list
       << " -master_nexus_path=" << FLAGS_nexus_root_path + FLAGS_master_path
       << " -work_mode=" << ((mode_ == kMapOnly) ? "map-only" : mode_str_);
    std::stringstream ss_stop;
    ss_stop << "source hdfs_env.sh; ./minion -jobid=" << job_id_ << " -nexus_addr=" << FLAGS_nexus_server_list
            << " -master_nexus_path=" << FLAGS_nexus_root_path + FLAGS_master_path
            << " -work_mode=" << ((mode_ == kMapOnly) ? "map-only" : mode_str_)
            << " -kill_task";
    ::baidu::galaxy::TaskDescription minion;
    minion.offset = 1;
    minion.binary = FLAGS_minion_path;
    minion.source_type = "kSourceTypeFTP";
    minion.start_cmd = ss.str().c_str();
    minion.stop_cmd = ss_stop.str().c_str();
    minion.requirement = galaxy_job.pod.requirement;
    minion.mem_isolation_type = "kMemIsolationCgroup";
    if (FLAGS_enable_cpu_soft_limit) {
        minion.cpu_isolation_type = "kCpuIsolationSoft";
    } else {
        minion.cpu_isolation_type = "kCpuIsolationHard";
    }
    galaxy_job.pod.tasks.push_back(minion);
    std::string minion_id;
    if (galaxy_->SubmitJob(galaxy_job, &minion_id)) {
        LOG(INFO, "galaxy job id: %s", minion_id.c_str());
        minion_id_ = minion_id;
        galaxy_job_ = galaxy_job;
        int retry_count = 10;
        while (minion_id_.empty() && retry_count-- > 0 ) {
            std::vector<galaxy::JobInformation> jobs;
            if (galaxy_->ListJobs(&jobs) ) {
                for (size_t i = 0; i < jobs.size(); i++) {
                    const galaxy::JobInformation& job_info = jobs[i];
                    if (job_info.job_name ==  galaxy_job_.job_name) {
                        minion_id_ = job_info.job_id;
                        LOG(INFO, "galaxy job id: %s", minion_id_.c_str());
                        break;
                    }
                }
            }
            sleep(3);
        }
        if (minion_id_.empty()) {
            LOG(INFO, "can not get galaxy job id");
            return kGalaxyError;
        }
        return kOk;
    }
    return kGalaxyError;
}

Status Gru::Kill() {
    LOG(INFO, "kill galaxy job: %s", minion_id_.c_str());
    if (minion_id_.empty()) {
        return kOk;
    }
    if (galaxy_->TerminateJob(minion_id_)) {
        return kOk;
    }
    return kGalaxyError;
}

Status Gru::Update(const std::string& priority,
                   int capacity) {
    ::baidu::galaxy::JobDescription job_desc = galaxy_job_;
    if (!priority.empty()) {
        job_desc.priority = priority;
    }
    if (capacity != -1) {
        job_desc.replica = capacity;
    }
    if (galaxy_->UpdateJob(minion_id_, job_desc)) {
        if (!priority.empty()) {
            galaxy_job_.priority = priority;
        }
        if (capacity != -1) {
            galaxy_job_.replica = capacity;
        }
        return kOk;
    }
    return kGalaxyError;
}

}
}

