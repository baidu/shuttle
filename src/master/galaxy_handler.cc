#include "galaxy_handler.h"

#include <sstream>
#include <algorithm>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "logging.h"

DECLARE_string(cluster_address);
DECLARE_string(galaxy_am_path);
DECLARE_string(cluster_user);
DECLARE_string(cluster_token);
DECLARE_int32(galaxy_deploy_step);
DECLARE_int32(max_minions_per_host);
DECLARE_string(galaxy_node_label);
DECLARE_string(cluster_pool);
DECLARE_bool(cpu_soft_limit);
DECLARE_bool(memory_soft_limit);
DECLARE_string(minion_path);
DECLARE_string(nexus_server_list);
DECLARE_string(nexus_root_path);
DECLARE_string(master_path);

namespace baidu {
namespace shuttle {

::baidu::galaxy::sdk::AppMaster* GalaxyHandler::galaxy_ = NULL;

GalaxyHandler::GalaxyHandler(JobDescriptor& job, const std::string& job_id, int node) :
        job_(job), job_id_(job_id), node_(node) {
    if (galaxy_ == NULL) {
        galaxy_ = ::baidu::galaxy::sdk::AppMaster::ConnectAppMaster(
                FLAGS_cluster_address, FLAGS_galaxy_am_path);
    }
    node_str_ = (job_.nodes(node).type() == kReduce) ? "m" : "r";
    node_str_ += boost::lexical_cast<std::string>(node);
    minion_name_ = job.name() + "_" + node_str_;
}

Status GalaxyHandler::Start() {
    ::baidu::galaxy::sdk::SubmitJobRequest request;
    ::baidu::galaxy::sdk::SubmitJobResponse response;
    request.user.user = FLAGS_cluster_user;
    request.user.token = FLAGS_cluster_token;
    request.job = PrepareGalaxyJob(minion_name_, job_.nodes(node_));
    if (galaxy_->SubmitJob(request, &response)) {
        minion_id_ = response.jobid;
        if (minion_id_.empty()) {
            LOG(WARNING, "cannot get galaxy job id: %s", job_id_.c_str());
            return kGalaxyError;
        }
        LOG(INFO, "galaxy job submitted: %s", minion_id_.c_str());
        return kOk;
    }
    LOG(WARNING, "galaxy report error when submit new job, %s",
            response.error_code.reason.c_str());
    return kGalaxyError;
}

Status GalaxyHandler::Kill() {
    LOG(DEBUG, "kill galaxy job: %s", minion_id_.c_str());
    if (minion_id_.empty()) {
        return kOk;
    }
    ::baidu::galaxy::sdk::RemoveJobRequest request;
    ::baidu::galaxy::sdk::RemoveJobResponse response;
    request.jobid = minion_id_;
    request.user.user = FLAGS_cluster_user;
    request.user.token = FLAGS_cluster_token;
    if (galaxy_->RemoveJob(request, &response)) {
        return kOk;
    }
    LOG(WARNING, "galaxy report error when kill job %s, %s",
            job_id_.c_str(), response.error_code.reason.c_str());
    return kGalaxyError;
}

Status GalaxyHandler::SetCapacity(int capacity) {
    ::baidu::galaxy::sdk::UpdateJobRequest request;
    ::baidu::galaxy::sdk::UpdateJobResponse response;
    request.user.user = FLAGS_cluster_user;
    request.user.token = FLAGS_cluster_token;
    request.jobid = minion_id_;
    request.job = PrepareGalaxyJob(minion_name_, job_.nodes(node_));
    request.job.deploy.replica = capacity;
    request.operate = ::baidu::galaxy::sdk::kUpdateJobStart;
    if (galaxy_->UpdateJob(request, &response)) {
        job_.mutable_nodes(node_)->set_capacity(capacity);
        return kOk;
    }
    LOG(WARNING, "galaxy report error when update job %s, %s",
            job_id_.c_str(), response.error_code.reason.c_str());
    return kGalaxyError;
}

Status GalaxyHandler::Load(const std::string& galaxy_jobid) {
    minion_id_ = galaxy_jobid;
    return kOk;
}

std::string GalaxyHandler::Dump() {
    return minion_id_;
}

::baidu::galaxy::sdk::JobDescription
GalaxyHandler::PrepareGalaxyJob(const std::string& name, const NodeConfig& node) {
    ::baidu::galaxy::sdk::JobDescription job;
    job.name = name + "@minion";
    job.type = ::baidu::galaxy::sdk::kJobBatch;
    job.version = "1.0.0";
    job.deploy.replica = std::min(node.capacity(), node.total() * 6 / 5);
    job.deploy.step = std::min(static_cast<uint32_t>(FLAGS_galaxy_deploy_step),
            job.deploy.replica);
    job.deploy.interval = 1;
    job.deploy.max_per_host = FLAGS_max_minions_per_host;
    job.deploy.tag = FLAGS_galaxy_node_label;
    boost::split(job.deploy.pools, FLAGS_cluster_pool, boost::is_any_of(","));
    job.deploy.update_break_count = 0;
    ::baidu::galaxy::sdk::PodDescription& pod = job.pod;
    pod.workspace_volum.size = (3L << 30);
    pod.workspace_volum.medium = ::baidu::galaxy::sdk::kDisk;
    pod.workspace_volum.exclusive = false;
    pod.workspace_volum.readonly = false;
    pod.workspace_volum.use_symlink = false;
    pod.workspace_volum.dest_path = "/home/shuttle";
    pod.workspace_volum.type = ::baidu::galaxy::sdk::kEmptyDir;
    ::baidu::galaxy::sdk::TaskDescription task;
    task.cpu.milli_core = node.millicores() + additional_millicores;
    task.cpu.excess = FLAGS_cpu_soft_limit;
    task.memory.size = node.memory() + additional_memory;
    task.memory.excess = FLAGS_memory_soft_limit;
    task.tcp_throt.recv_bps_quota = (80L << 20);
    task.tcp_throt.recv_bps_excess = true;
    task.tcp_throt.send_bps_quota = (80L << 20);
    task.tcp_throt.send_bps_excess = true;
    task.blkio.weight = 50;
    ::baidu::galaxy::sdk::PortRequired port_req;
    port_req.port = "dynamic";
    port_req.port_name = "NFS_CLIENT_PORT";
    task.ports.push_back(port_req);
    port_req.port_name = "MINION_PORT";
    task.ports.push_back(port_req);
    task.exe_package.package.source_path = FLAGS_minion_path;
    task.exe_package.package.dest_path = ".";
    task.exe_package.package.version = "1.0.0";
    // TODO Need to compatible with minion
    // XXX Changed: app_package is separated by comma to store more values
    std::string app_package, cache_archive;
    ::google::protobuf::RepeatedPtrField<const std::string>::iterator it;
    for (it = job_.files().begin(); it != job_.files().end(); ++it) {
        app_package += (*it) + ",";
    }
    app_package.erase(app_package.end() - 1);
    // XXX Changed: work_mode --> node
    std::stringstream ss;
    ss << "app_package=" << app_package << " cache_archive=" << job_.cache_archive()
       << " ./minion_boot.sh -jobid=" << job_id_ << " -nexus_addr=" << FLAGS_nexus_server_list
       << " -master_nexus_path=" << FLAGS_nexus_root_path + FLAGS_master_path
       << " -node=" << node_;
    std::stringstream ss_stop;
    ss_stop << "source hdfs_env.sh; ./minion -jobid=" << job_id_
            << " -nexus_addr=" << FLAGS_nexus_server_list
            << " -master_nexus_path=" << FLAGS_nexus_root_path + FLAGS_master_path
            << " -node=" << node_ << " -kill_task";
    task.exe_package.start_cmd = ss.str();
    task.exe_package.stop_cmd = ss_stop.str();
    task.data_package.reload_cmd = task.exe_package.start_cmd;
    return job;
}

}
}

