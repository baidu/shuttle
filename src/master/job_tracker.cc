#include "job_tracker.h"

#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include "timer.h"
#include "resource_manager.h"

DECLARE_int32(galaxy_deploy_step);
DECLARE_string(minion_path);
DECLARE_int32(timeout_bound);
DECLARE_int32(replica_num);
DECLARE_int32(replica_begin);

namespace baidu {
namespace shuttle {

const std::string shuttle_label = "map_reduce_shuttle";

JobTracker::JobTracker(::baidu::galaxy::Galaxy* galaxy_sdk, const JobDescriptor& job) :
                      sdk_(galaxy_sdk),
                      job_descriptor_(job) {
    char time_str[32];
    ::baidu::common::timer::now_time_str(time_str, 32);
    job_id_ = time_str;
    job_id_ += boost::lexical_cast<std::string>(random());
    resource_ = new ResourceManager();
}

JobTracker::~JobTracker() {
    delete resource_;
    MutexLock lock(&alloc_mu_);
    for (std::list<AllocateItem*>::iterator it = allocation_table_.begin();
            it != allocation_table_.end(); ++it) {
        delete *it;
    }
}

Status JobTracker::Start() {
    // TODO Start reduce at proper time
    ::baidu::galaxy::JobDescription galaxy_job;
    galaxy_job.job_name = job_descriptor_.name() + "@minion";
    galaxy_job.type = "kBatch";
    galaxy_job.priority = "kOnline";
    galaxy_job.replica = job_descriptor_.map_capacity();
    galaxy_job.label = shuttle_label;
    galaxy_job.deploy_step = FLAGS_galaxy_deploy_step;
    galaxy_job.pod.requirement.millicores = job_descriptor_.millicores();
    galaxy_job.pod.requirement.memory = job_descriptor_.memory();
    ::baidu::galaxy::TaskDescription minion;
    minion.offset = 1;
    minion.binary = FLAGS_minion_path;
    minion.source_type = "kSourceTypeFTP";
    // TODO minion flags
    minion.start_cmd = "./minion";
    minion.requirement = galaxy_job.pod.requirement;
    galaxy_job.pod.tasks.push_back(minion);
    std::string minion_id;
    if (sdk_->SubmitJob(galaxy_job, &minion_id)) {
        monitor_.AddTask(boost::bind(&JobTracker::KeepMonitoring, this));
        MutexLock lock(&mu_);
        map_minion_ = minion_id;
        return kOk;
    }
    return kGalaxyError;
}

Status JobTracker::Update(const std::string& priority,
                          int map_capacity,
                          int reduce_capacity) {
    if (!map_minion_.empty()) {
        mu_.Lock();
        ::baidu::galaxy::JobDescription map_desc = map_description_;
        mu_.Unlock();
        map_desc.priority = priority;
        map_desc.replica = map_capacity;
        if (sdk_->UpdateJob(map_minion_, map_desc)) {
            MutexLock lock(&mu_);
            map_description_.priority = priority;
            map_description_.replica = map_capacity;
        } else {
            return kGalaxyError;
        }
    }

    if (!reduce_minion_.empty()) {
        mu_.Lock();
        ::baidu::galaxy::JobDescription reduce_desc = reduce_description_;
        mu_.Unlock();
        reduce_desc.priority = priority;
        reduce_desc.replica = reduce_capacity;
        if (sdk_->UpdateJob(reduce_minion_, reduce_desc)) {
            MutexLock lock(&mu_);
            reduce_description_.priority = priority;
            reduce_description_.replica = reduce_capacity;
        } else {
            return kGalaxyError;
        }
    }
    return kOk;
}

Status JobTracker::Kill() {
    MutexLock lock(&mu_);
    if (!map_minion_.empty() && !sdk_->TerminateJob(map_minion_)) {
        return kGalaxyError;
    }
    if (!reduce_minion_.empty() && !sdk_->TerminateJob(reduce_minion_)) {
        return kGalaxyError;
    }
    return kOk;
}

ResourceItem* JobTracker::Assign(const std::string& endpoint) {
    static int last_no = -1;
    static int last_attempt = 0;
    if (resource_->SumOfItem() - last_no > FLAGS_replica_begin &&
            last_attempt <= FLAGS_replica_num) {
        // TODO Need consideration
        AllocateItem* alloc = new AllocateItem();
        alloc->resource_no = last_no;
        alloc->attempt = ++last_attempt;
        alloc->endpoint = endpoint;
        alloc->alloc_time = std::time(NULL);
        return resource_->GetCertainItem(alloc->resource_no);
    }
    ResourceItem* cur = resource_->GetItem();
    AllocateItem* alloc = new AllocateItem();
    alloc->resource_no = cur->no;
    alloc->attempt = 1;
    alloc->endpoint = endpoint;
    alloc->alloc_time = std::time(NULL);
    cur->attempt = alloc->attempt;
    // TODO Need Consideration
    last_no = cur->no;
    last_attempt = cur->attempt;

    MutexLock lock(&alloc_mu_);
    allocation_table_.push_back(alloc);
    time_heap_.push(alloc);
    return cur;
}

Status JobTracker::FinishTask(int no, int attempt, TaskState state) {
    // TODO finish a map/reduce task
    return kOk;
}

void JobTracker::KeepMonitoring() {
    // TODO Check top of heap, manage timeout job
    time_t now = std::time(NULL);
    AllocateItem* cur = NULL;
    while (now - ((cur = time_heap_.top())->alloc_time) > FLAGS_timeout_bound) {
        // TODO Return back the resource
    }
    monitor_.DelayTask(FLAGS_timeout_bound * 1000,
                       boost::bind(&JobTracker::KeepMonitoring, this));
}

}
}

