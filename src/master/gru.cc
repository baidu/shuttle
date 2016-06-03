#include "gru.h"

#include <sstream>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>
#include <vector>
#include "logging.h"
#include "util.h"

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_string(nexus_server_list);
DECLARE_string(nexus_root_path);
DECLARE_string(master_path);
DECLARE_bool(enable_cpu_soft_limit);
DECLARE_bool(enable_memory_soft_limit);
DECLARE_string(galaxy_node_label);
DECLARE_string(galaxy_user);
DECLARE_string(galaxy_token);
DECLARE_string(galaxy_pool);
DECLARE_int32(max_minions_per_host);

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

Gru::Gru(::baidu::galaxy::sdk::AppMaster* galaxy, JobDescriptor* job,
         const std::string& job_id, WorkMode mode) :
        galaxy_(galaxy), job_(job), job_id_(job_id), mode_(mode) {
    mode_str_ = ((mode == kReduce) ? "reduce" : "map");
    minion_name_ = job->name() + "_" + mode_str_;
}

Status Gru::Start() {
    ::baidu::galaxy::sdk::SubmitJobRequest galaxy_job;
    galaxy_job.user.user = FLAGS_galaxy_user;
    galaxy_job.user.token = FLAGS_galaxy_token;
    galaxy_job.hostname = ::baidu::common::util::GetLocalHostName();
    galaxy_job.job.deploy.pools.push_back(FLAGS_galaxy_pool);
    galaxy_job.job.name = minion_name_ + "@minion";
    galaxy_job.job.type = ::baidu::galaxy::sdk::kJobBatch;
    galaxy_job.job.deploy.replica = (mode_ == kReduce) ? job_->reduce_capacity() : job_->map_capacity();
    galaxy_job.job.deploy.step = FLAGS_galaxy_deploy_step;
    galaxy_job.job.deploy.interval = 1;
    galaxy_job.job.deploy.max_per_host = FLAGS_max_minions_per_host;
    galaxy_job.job.deploy.update_break_count = 0;
    galaxy_job.job.version = "1.0.0";
    galaxy_job.job.run_user = "galaxy";
    if (!FLAGS_galaxy_node_label.empty()) {
        galaxy_job.job.deploy.tag = FLAGS_galaxy_node_label;
    }
    ::baidu::galaxy::sdk::PodDescription & pod_desc = galaxy_job.job.pod;
    pod_desc.workspace_volum.size = (3L << 30);
    pod_desc.workspace_volum.medium = ::baidu::galaxy::sdk::kDisk;
    pod_desc.workspace_volum.exclusive = false;
    pod_desc.workspace_volum.readonly = false;
    pod_desc.workspace_volum.use_symlink = false;
    pod_desc.workspace_volum.dest_path = "/home/shuttle";
    pod_desc.workspace_volum.type = ::baidu::galaxy::sdk::kEmptyDir;

    ::baidu::galaxy::sdk::TaskDescription task_desc;
    if (mode_str_ == "map") {
        task_desc.cpu.milli_core = job_->millicores() + additional_map_millicores;
    } else {
        task_desc.cpu.milli_core = job_->millicores() + additional_reduce_millicores;
    }
    task_desc.memory.size = job_->memory() +
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
    if (!job_->input_dfs().user().empty()) {
        ss << "hadoop_job_ugi=" << job_->input_dfs().user()
           << "," << job_->input_dfs().password()
           << " fs_default_name=hdfs://" << job_->input_dfs().host()
           << ":" << job_->input_dfs().port() << " "; 
    }
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
    task_desc.exe_package.package.source_path = FLAGS_minion_path;
    task_desc.exe_package.package.dest_path = ".";
    task_desc.exe_package.package.version = "1.0";
    task_desc.exe_package.start_cmd = ss.str().c_str();
    task_desc.exe_package.stop_cmd = ss_stop.str().c_str();
    task_desc.tcp_throt.recv_bps_quota = (50L << 20);
    task_desc.tcp_throt.send_bps_quota = (50L << 20);
    task_desc.blkio.weight = 100;
    ::baidu::galaxy::sdk::PortRequired port_req;
    port_req.port = "dynamic";
    port_req.port_name = "NFS_CLIENT_PORT";
    task_desc.ports.push_back(port_req);
    port_req.port = "dynamic";
    port_req.port_name = "MINION_PORT";
    task_desc.ports.push_back(port_req);
    if (FLAGS_enable_cpu_soft_limit) {
        task_desc.cpu.excess = true;
    } else {
        task_desc.cpu.excess = false;
    }
    if (FLAGS_enable_memory_soft_limit) {
        task_desc.memory.excess = true;
    } else {
        task_desc.memory.excess = false;
    }
    pod_desc.tasks.push_back(task_desc);
    std::string minion_id;
    ::baidu::galaxy::sdk::SubmitJobResponse rsps;
    if (galaxy_->SubmitJob(galaxy_job, &rsps)) {
        minion_id = rsps.jobid;
        LOG(INFO, "galaxy job id: %s", minion_id.c_str());
        minion_id_ = minion_id;
        galaxy_job_ = galaxy_job;
        if (minion_id_.empty()) {
            LOG(INFO, "can not get galaxy job id");
            return kGalaxyError;
        }
        return kOk;
    } else {
        LOG(WARNING, "galaxy error: %s", rsps.error_code.reason.c_str());
    }
    return kGalaxyError;
}

Status Gru::Kill() {
    LOG(INFO, "kill galaxy job: %s", minion_id_.c_str());
    if (minion_id_.empty()) {
        return kOk;
    }
    ::baidu::galaxy::sdk::RemoveJobRequest rqst;
    ::baidu::galaxy::sdk::RemoveJobResponse rsps;
    rqst.jobid = minion_id_;
    rqst.user = galaxy_job_.user;
    rqst.hostname = ::baidu::common::util::GetLocalHostName();
    if (galaxy_->RemoveJob(rqst, &rsps)) {
        return kOk;
    } else {
        LOG(WARNING, "galaxy error: %s", rsps.error_code.reason.c_str());
    }
    return kGalaxyError;
}

Status Gru::Update(const std::string& priority,
                   int capacity) {
    ::baidu::galaxy::sdk::JobDescription job_desc = galaxy_job_.job;
    if (!priority.empty()) {
        //job_desc.priority = priority;
    }
    if (capacity != -1) {
        job_desc.deploy.replica = capacity;
    }
    ::baidu::galaxy::sdk::UpdateJobRequest rqst;
    ::baidu::galaxy::sdk::UpdateJobResponse rsps;
    rqst.user = galaxy_job_.user;
    rqst.job = job_desc;
    rqst.jobid = minion_id_;
    rqst.operate = ::baidu::galaxy::sdk::kUpdateJobDefault;
    rqst.hostname = ::baidu::common::util::GetLocalHostName();
    if (galaxy_->UpdateJob(rqst, &rsps)) {
        if (!priority.empty()) {
            //galaxy_job_.priority = priority;
        }
        if (capacity != -1) {
            galaxy_job_.job.deploy.replica = capacity;
        }
        return kOk;
    } else {
        LOG(WARNING, "galaxy error: %s", rsps.error_code.reason.c_str());
    }
    return kGalaxyError;
}

}
}

