#include "galaxy_handler.h"

#include <sstream>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_string(nexus_server_list);
DECLARE_string(nexus_root_path);
DECLARE_string(master_path);
DECLARE_string(galaxy_address);

namespace baidu {
namespace shuttle {

static const int64_t default_additional_memory = 1024l * 1024 * 1024;
static const int default_additional_millicores = 0;

int GalaxyHandler::additional_millicores = default_additional_millicores;
int64_t GalaxyHandler::additional_memory = default_additional_memory;

GalaxyHandler::GalaxyHandler(JobDescriptor& job, const std::string& job_id, int node) :
        galaxy_(NULL), job_(job), job_id_(job_id), node_(node) {
    galaxy_ = ::baidu::galaxy::Galaxy::ConnectGalaxy(FLAGS_galaxy_address);
    node_str_ = (job_.nodes(node).type() == kReduce) ? "m" : "r";
    node_str_ += boost::lexical_cast<std::string>(node);
    minion_name_ = job.name() + "_" + node_str_;
}

Status GalaxyHandler::Start() {
    const NodeConfig& cur_node = job_.nodes(node_);
    ::baidu::galaxy::JobDescription galaxy_job;
    galaxy_job.job_name = minion_name_ + "@minion";
    galaxy_job.type = "kLongRun";
    galaxy_job.priority = "kOnline";
    galaxy_job.replica = cur_node.capacity();
    galaxy_job.deploy_step = FLAGS_galaxy_deploy_step;
    galaxy_job.pod.version = "1.0.0";
    galaxy_job.pod.requirement.millicores = cur_node.millicores() + additional_millicores;
    galaxy_job.pod.requirement.memory = cur_node.memory() + additional_memory;
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
    ::baidu::galaxy::TaskDescription minion;
    minion.offset = 1;
    minion.binary = FLAGS_minion_path;
    minion.source_type = "kSourceTypeFTP";
    minion.start_cmd = ss.str().c_str();
    minion.stop_cmd = ss_stop.str().c_str();
    minion.requirement = galaxy_job.pod.requirement;
    minion.mem_isolation_type = "kMemIsolationLimit";
    minion.cpu_isolation_type = "kCpuIsolationSoft";
    galaxy_job.pod.tasks.push_back(minion);
    std::string minion_id;
    if (galaxy_->SubmitJob(galaxy_job, &minion_id)) {
        minion_id_ = minion_id;
        galaxy_job_ = galaxy_job;
        return kOk;
    }
    return kGalaxyError;
}

Status GalaxyHandler::Kill() {
    if (minion_id_.empty()) {
        return kOk;
    }
    if (!galaxy_->TerminateJob(minion_id_)) {
        return kOk;
    }
    return kGalaxyError;
}

Status GalaxyHandler::Update(const std::string& priority,
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

