#include "gru.h"

#include <sstream>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_string(nexus_server_list);

namespace baidu {
namespace shuttle {

static const int64_t default_additional_map_memory = 1024l * 1024 * 1024;
static const int64_t default_additional_reduce_memory = 1024l * 1024 * 1024;
static const int default_additional_millicores = 1000;

int Gru::additional_millicores = default_additional_millicores;
int64_t Gru::additional_map_memory = default_additional_map_memory;
int64_t Gru::additional_reduce_memory = default_additional_reduce_memory;

Gru::Gru(::baidu::galaxy::Galaxy* galaxy, JobDescriptor* job,
         const std::string& job_id, WorkMode mode) :
        galaxy_(galaxy), job_(job), job_id_(job_id), mode_(mode) {
    mode_str_ = ((mode == kReduce) ? "_reduce" : "_map");
    minion_name_ = job->name() + mode_str_;
}

Status Gru::Start() {
    ::baidu::galaxy::JobDescription galaxy_job;
    galaxy_job.job_name = minion_name_ + "@minion";
    galaxy_job.type = "kBatch";
    galaxy_job.priority = "kOnline";
    galaxy_job.replica = job_->map_capacity();
    galaxy_job.deploy_step = FLAGS_galaxy_deploy_step;
    galaxy_job.pod.requirement.millicores = job_->millicores() + additional_millicores;
    galaxy_job.pod.requirement.memory = job_->memory() +
        ((mode_ == kReduce) ? additional_reduce_memory : additional_map_memory);
    std::string app_package, cache_archive;
    int file_size = job_->files().size();
    for (int i = 0; i < file_size; ++i) {
        const std::string& file = job_->files(i);
        if (boost::starts_with(file, "hdfs://")) {
            cache_archive = file;
        } else {
            app_package = file;
        }
    }
    std::stringstream ss;
    ss << "app_package=" << job_->files(0) << " ./minion_boot.sh"
       << " -jobid=" << job_id_ << " -nexus_addr=" << FLAGS_nexus_server_list
       << " -work_mode=" << ((mode_ == kMapOnly) ? "map-only" : mode_str_);
    ::baidu::galaxy::TaskDescription minion;
    minion.offset = 1;
    minion.binary = FLAGS_minion_path;
    minion.source_type = "kSourceTypeFTP";
    minion.start_cmd = ss.str().c_str();
    minion.requirement = galaxy_job.pod.requirement;
    galaxy_job.pod.tasks.push_back(minion);
    std::string minion_id;
    if (galaxy_->SubmitJob(galaxy_job, &minion_id)) {
        minion_id_ = minion_id;
        galaxy_job_ = galaxy_job;
        return kOk;
    }
    return kGalaxyError;
}

Status Gru::Kill() {
    if (minion_id_.empty()) {
        return kOk;
    }
    if (!galaxy_->TerminateJob(minion_id_)) {
        return kOk;
    }
    return kGalaxyError;
}

Status Gru::Update(const std::string& priority,
                   int capacity) {
    ::baidu::galaxy::JobDescription job_desc = galaxy_job_;
    job_desc.priority = priority;
    job_desc.replica = capacity;
    if (galaxy_->UpdateJob(minion_id_, job_desc)) {
        galaxy_job_.priority = priority;
        galaxy_job_.replica = capacity;
        return kOk;
    }
    return kGalaxyError;
}

}
}

