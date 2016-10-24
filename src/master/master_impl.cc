#include "master_impl.h"

#include <vector>
#include <sstream>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <sys/utsname.h>
#include <gflags/gflags.h>
#include <boost/bind.hpp>
#include <snappy.h>
#include "timer.h"
#include "logging.h"
#include "resource_manager.h"

DECLARE_string(nexus_server_list);
DECLARE_bool(recovery);
DECLARE_string(nexus_root_path);
DECLARE_string(master_lock_path);
DECLARE_string(master_path);
DECLARE_string(master_port);
DECLARE_int32(gc_interval);
DECLARE_int32(backup_interval);

namespace baidu {
namespace shuttle {

MasterImpl::MasterImpl() {
    srand(time(NULL));
    nexus_ = new ::galaxy::ins::sdk::InsSDK(FLAGS_nexus_server_list);
    gc_.AddTask(boost::bind(&MasterImpl::KeepGarbageCollecting, this));
}

MasterImpl::~MasterImpl() {
    MutexLock lock(&(tracker_mu_));
    std::map<std::string, JobTracker*>::iterator it;
    for (it = job_trackers_.begin(); it != job_trackers_.end(); ++it) {
        delete it->second;
    }
    for (it = dead_trackers_.begin(); it != dead_trackers_.end(); ++it) {
        delete it->second;
    }
    delete nexus_;
}

void MasterImpl::Init() {
    AcquireMasterLock();
    LOG(INFO, "master alive, recovering");
    if (FLAGS_recovery) {
        Reload();
        LOG(INFO, "master recovered");
    }
}

void MasterImpl::SubmitJob(::google::protobuf::RpcController* /*controller*/,
                           const ::baidu::shuttle::SubmitJobRequest* request,
                           ::baidu::shuttle::SubmitJobResponse* response,
                           ::google::protobuf::Closure* done) {
    const JobDescriptor& job = request->job();
    LOG(INFO, "new job submitted");
    LOG(INFO, "=== job details ===");
    LOG(INFO, "%s", job.DebugString().c_str());
    LOG(INFO, "==== end of job details ==");
    JobTracker* jobtracker = new JobTracker(job);
    jobtracker->RegisterFinishedCallback(
            boost::bind(&MasterImpl::RetractJob, this, jobtracker->GetJobId()));
    Status status = jobtracker->Start();
    const std::string& job_id = jobtracker->GetJobId();
    if (status == kOk) {
        MutexLock lock(&(tracker_mu_));
        job_trackers_[job_id] = jobtracker;
    } else {
        MutexLock lock(&(tracker_mu_));
        dead_trackers_[job_id] = jobtracker;
    }
    response->set_status(status);
    response->set_jobid(job_id);
    done->Run();
}

void MasterImpl::UpdateJob(::google::protobuf::RpcController* /*controller*/,
                           const ::baidu::shuttle::UpdateJobRequest* request,
                           ::baidu::shuttle::UpdateJobResponse* response,
                           ::google::protobuf::Closure* done) {
    const std::string& job_id = request->jobid();
    size_t size = request->capacities().size();
    std::vector<UpdateItem> nodes;
    nodes.resize(size);
    for (size_t i = 0; i < size; ++i) {
        nodes[i].node = request->capacities(i).node();
        nodes[i].capacity = request->capacities(i).capacity();
    }
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        Status status = jobtracker->Update(nodes);
        response->set_status(status);
    } else {
        LOG(WARNING, "try to update an inexist job: %s", job_id.c_str());
        response->set_status(kNoSuchJob);
    }
    done->Run();
}

void MasterImpl::KillJob(::google::protobuf::RpcController* /*controller*/,
                         const ::baidu::shuttle::KillJobRequest* request,
                         ::baidu::shuttle::KillJobResponse* response,
                         ::google::protobuf::Closure* done) {
    const std::string& job_id = request->jobid();
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        Status status = jobtracker->Kill();
        response->set_status(status);
    } else {
        LOG(WARNING, "try to kill an inexist job: %s", job_id.c_str());
        response->set_status(kNoSuchJob);
    }
    done->Run();
}

void MasterImpl::ListJobs(::google::protobuf::RpcController* /*controller*/,
                          const ::baidu::shuttle::ListJobsRequest* request,
                          ::baidu::shuttle::ListJobsResponse* response,
                          ::google::protobuf::Closure* done) {
    std::map<std::string, JobTracker*>::iterator it;
    {
        MutexLock lock1(&(tracker_mu_));
        for (it = job_trackers_.begin(); it != job_trackers_.end(); ++it) {
            JobTracker* jobtracker = it->second;
            JobOverview* job = response->add_jobs();
            job->mutable_desc()->CopyFrom(jobtracker->GetJobDescriptor());
            job->set_jobid(it->first);
            job->set_state(jobtracker->GetState());
            std::vector<TaskStatistics> stats;
            jobtracker->GetStatistics(stats);
            for (std::vector<TaskStatistics>::iterator jt = stats.begin();
                    jt != stats.end(); ++jt) {
                job->add_stats()->CopyFrom(*jt);
            }
            job->set_start_time(jobtracker->GetStartTime());
            job->set_finish_time(jobtracker->GetFinishTime());
        }
    }
    if (request->all()) {
        MutexLock lock2(&(dead_mu_));
        for (it = dead_trackers_.begin(); it != dead_trackers_.end(); ++it) {
            JobTracker* jobtracker = it->second;
            JobOverview* job = response->add_jobs();
            job->mutable_desc()->CopyFrom(jobtracker->GetJobDescriptor());
            job->set_jobid(it->first);
            job->set_state(jobtracker->GetState());
            std::vector<TaskStatistics> stats;
            jobtracker->GetStatistics(stats);
            for (std::vector<TaskStatistics>::iterator jt = stats.begin();
                    jt != stats.end(); ++jt) {
                job->add_stats()->CopyFrom(*jt);
            }
            job->set_start_time(jobtracker->GetStartTime());
            job->set_finish_time(jobtracker->GetFinishTime());
        }
    }
    done->Run();
}

void MasterImpl::ShowJob(::google::protobuf::RpcController* /*controller*/,
                         const ::baidu::shuttle::ShowJobRequest* request,
                         ::baidu::shuttle::ShowJobResponse* response,
                         ::google::protobuf::Closure* done) {
    const std::string& job_id = request->jobid();
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker == NULL && request->all()) {
        MutexLock lock(&(dead_mu_));
        std::map<std::string, JobTracker*>::iterator it = dead_trackers_.find(job_id);
        if (it != dead_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        response->set_status(kOk);
        JobOverview* job = response->mutable_job();
        job->mutable_desc()->CopyFrom(jobtracker->GetJobDescriptor());
        job->set_jobid(job_id);
        job->set_state(jobtracker->GetState());
        std::vector<TaskStatistics> stats;
        jobtracker->GetStatistics(stats);
        for (std::vector<TaskStatistics>::iterator it = stats.begin();
                it != stats.end(); ++it) {
            job->add_stats()->CopyFrom(*it);
        }
        job->set_start_time(jobtracker->GetStartTime());
        job->set_finish_time(jobtracker->GetFinishTime());

        std::vector<TaskOverview> tasks;
        jobtracker->GetTaskOverview(tasks);
        for (std::vector<TaskOverview>::iterator it = tasks.begin();
                it != tasks.end(); ++it) {
            response->add_tasks()->CopyFrom(*it);
        }
        // TODO Query progress here
    } else {
        LOG(WARNING, "try to access an inexist job: %s", job_id.c_str());
        response->set_status(kNoSuchJob);
    }
    done->Run();
}

void MasterImpl::AssignTask(::google::protobuf::RpcController* /*controller*/,
                            const ::baidu::shuttle::AssignTaskRequest* request,
                            ::baidu::shuttle::AssignTaskResponse* response,
                            ::google::protobuf::Closure* done) {
    const std::string& job_id = request->jobid();
    
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        Status status = kOk;
        ResourceItem* resource = jobtracker->Assign(request->node(), request->endpoint(), &status);
        response->set_status(status);
        if (resource == NULL) {
            done->Run();
            return;
        }
        response->mutable_job()->CopyFrom(jobtracker->GetJobDescriptor());
        TaskInfo* task = response->mutable_task();
        task->set_task_id(resource->no);
        task->set_attempt_id(resource->attempt);
        if (resource->type == kFileItem) {
            TaskInput* input = task->mutable_input();
            input->set_input_file(resource->input_file);
            input->set_input_offset(resource->offset);
            input->set_input_size(resource->size);
        }
        task->set_node(request->node());
        delete resource;
    } else {
        {
            MutexLock lock(&(dead_mu_));
            std::map<std::string, JobTracker*>::iterator it = dead_trackers_.find(job_id);
            if (it != dead_trackers_.end()) {
                jobtracker = it->second;
            }
        }
        if (jobtracker != NULL) {
            response->set_status(kNoMore);
        } else {
            LOG(WARNING, "assign task failed: job inexist: %s", job_id.c_str());
            response->set_status(kNoSuchJob);
        }
    }
    done->Run();
}

void MasterImpl::FinishTask(::google::protobuf::RpcController* /*controller*/,
                            const ::baidu::shuttle::FinishTaskRequest* request,
                            ::baidu::shuttle::FinishTaskResponse* response,
                            ::google::protobuf::Closure* done) {
    const std::string& job_id = request->jobid();
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        Status status = jobtracker->Finish(request->node(),
                request->task_id(), request->attempt_id(), request->task_state());
        response->set_status(status);
    } else {
        {
            MutexLock lock(&(dead_mu_));
            std::map<std::string, JobTracker*>::iterator it = dead_trackers_.find(job_id);
            if (it != dead_trackers_.end()) {
                jobtracker = it->second;
            }
        }
        if (jobtracker != NULL) {
            response->set_status(kOk);
        } else {
            LOG(WARNING, "finish task failed: job inexist: %s", job_id.c_str());
            response->set_status(kNoSuchJob);
        }
    }
    done->Run();
}

void MasterImpl::AcquireMasterLock() {
    std::string master_lock = FLAGS_nexus_root_path + FLAGS_master_lock_path;
    ::galaxy::ins::sdk::SDKError err;
    nexus_->RegisterSessionTimeout(&OnMasterSessionTimeout, this);
    bool ret = nexus_->Lock(master_lock, &err);
    assert(ret && err == ::galaxy::ins::sdk::kOK);
    std::string master_key = FLAGS_nexus_root_path + FLAGS_master_path;
    std::string master_endpoint = SelfEndpoint();
    ret = nexus_->Put(master_key, master_endpoint, &err);
    assert(ret && err == ::galaxy::ins::sdk::kOK);
    ret = nexus_->Watch(master_lock, &OnMasterLockChange, this, &err);
    assert(ret && err == ::galaxy::ins::sdk::kOK);
    LOG(INFO, "master lock acquired. %s -> %s", master_key.c_str(), master_endpoint.c_str());
}

void MasterImpl::OnMasterSessionTimeout(void* ctx) {
    MasterImpl* master = static_cast<MasterImpl*>(ctx);
    master->OnSessionTimeout();
}

void MasterImpl::OnSessionTimeout() {
    LOG(FATAL, "master lost session with nexus, die");
    abort();
}

void MasterImpl::OnMasterLockChange(const ::galaxy::ins::sdk::WatchParam& param,
                                    ::galaxy::ins::sdk::SDKError /*err*/) {
    MasterImpl* master = static_cast<MasterImpl*>(param.context);
    master->OnLockChange(param.value);
}

void MasterImpl::OnLockChange(const std::string& lock_session_id) {
    std::string self_session_id = nexus_->GetSessionID();
    if (self_session_id != lock_session_id) {
        LOG(FATAL, "master lost lock, die");
        abort();
    }
}

std::string MasterImpl::SelfEndpoint() {
    std::string hostname = "";
    struct utsname buf;
    if (0 != uname(&buf)) {
        *buf.nodename = '\0';
    }
    hostname = buf.nodename;
    return hostname + ":" + FLAGS_master_port;
}

void MasterImpl::RetractJob(const std::string& jobid) {
    MutexLock lock(&(tracker_mu_));
    MutexLock lock2(&(dead_mu_));
    std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(jobid);
    if (it == job_trackers_.end()) {
        LOG(WARNING, "retract job failed: job inexist: %s", jobid.c_str());
        return;
    }

    JobTracker* jobtracker = it->second;
    job_trackers_.erase(it);
    dead_trackers_[jobid] = jobtracker;
}

void MasterImpl::KeepGarbageCollecting() {
    MutexLock lock(&(dead_mu_));
    std::set<std::string> gc_jobs;
    for (std::map<std::string, JobTracker*>::iterator it = dead_trackers_.begin();
            it != dead_trackers_.end(); ++it) {
        JobTracker* jobtracker = it->second;
        const std::string& jobid = it->first;
        int32_t interval_seconds = jobtracker->GetFinishTime() - common::timer::now_time();
        if (interval_seconds < 0 || interval_seconds > FLAGS_gc_interval) {
            gc_jobs.insert(jobid);
        }
    }
    std::set<std::string>::iterator it;
    for (it = gc_jobs.begin(); it != gc_jobs.end(); it++) {
        const std::string& jobid = *it;
        std::map<std::string, JobTracker*>::iterator jt = dead_trackers_.find(jobid);
        if (jt != dead_trackers_.end()) {
            JobTracker* jobtracker = jt->second;
            dead_trackers_.erase(jobid);
            delete jobtracker;
            LOG(INFO, "[gc] remove dead jobtracker: %s", jobid.c_str());
            RemoveJobFromNexus(jobid);
        }
    }
    gc_.DelayTask(FLAGS_gc_interval * 1000,
                  boost::bind(&MasterImpl::KeepGarbageCollecting, this));
}

void MasterImpl::KeepDataPersistence() {
    // TODO Maybe do diff here to reduce pressure
    tracker_mu_.Lock();
    for (std::map<std::string, JobTracker*>::iterator it = job_trackers_.begin();
            it != job_trackers_.end(); ++it) {
        SaveJobToNexus(it->second);
    }
    tracker_mu_.Unlock();

    dead_mu_.Lock();
    for (std::map<std::string, JobTracker*>::iterator it = dead_trackers_.begin();
         it != dead_trackers_.end(); ++it) {
        if (saved_dead_jobs_.find(it->first) == saved_dead_jobs_.end()) {
            if (SaveJobToNexus(it->second)) {
                saved_dead_jobs_.insert(it->first);
            }
        }
    }
    dead_mu_.Unlock();

    gc_.DelayTask(FLAGS_backup_interval, boost::bind(&MasterImpl::KeepDataPersistence, this));
}

void MasterImpl::Reload() {
    JobCollection jc;
    while (GetJobFromNexus(jc)) {
        JobTracker* jobtracker = new JobTracker(jc.job());
        const std::string& jobid = jobtracker->GetJobId();
        jobtracker->RegisterFinishedCallback(
                boost::bind(&MasterImpl::RetractJob, this, jobid));
        if (jobtracker->Load(jc.tracker()) == kInvalidArg) {
            continue;
        }
        JobState state = jobtracker->GetState();
        if (state == kPending || state == kRunning) {
            job_trackers_[jobid] = jobtracker;
        } else {
            dead_trackers_[jobid] = jobtracker;
        }
    }
    gc_.AddTask(boost::bind(&MasterImpl::KeepDataPersistence, this));
}

Status MasterImpl::Load(const std::string& compressed, JobCollection& jc) {
    std::string serialized;
    snappy::Uncompress(compressed.data(), compressed.size(), &serialized);
    if (!jc.ParseFromString(serialized)) {
        return kInvalidArg;
    }
    return kOk;
}

std::string MasterImpl::Dump(const JobCollection& jc) {
    std::string serialized;
    jc.SerializeToString(&serialized);
    std::string compressed;
    snappy::Compress(serialized.data(), serialized.size(), &compressed);
    return compressed;
}

bool MasterImpl::GetJobFromNexus(JobCollection& jc) {
    static ::galaxy::ins::sdk::ScanResult* result = nexus_->Scan(
            FLAGS_nexus_root_path + "job_", FLAGS_nexus_root_path + "job`");
    Status status = kUnKnown;
    // This inner loop filters invalid serialized data string
    while (status != kOk) {
        if (result->Done()) {
            return false;
        }
        status = Load(result->Value(), jc);
        result->Next();
    }
    return true;
}

bool MasterImpl::SaveJobToNexus(JobTracker* jobtracker) {
    JobCollection jc;
    jc.mutable_job()->CopyFrom(jobtracker->GetJobDescriptor());
    jc.set_tracker(jobtracker->Dump());
    const std::string& raw_data = Dump(jc);
    const std::string& jobid = jobtracker->GetJobId();
    bool ok = nexus_->Put(FLAGS_nexus_root_path + jobid, raw_data, NULL);
    LOG(INFO, "[%s] job persistence saved %u bytes: %s",
            ok ? "OK" : "FAIL", jobid.c_str(), raw_data.size());
    return ok;
}

bool MasterImpl::RemoveJobFromNexus(const std::string& jobid) {
    bool ok = nexus_->Delete(FLAGS_nexus_root_path + jobid, NULL);
    LOG(INFO, "[%s] remove job from nexus", ok ? "OK": "FAIL", jobid.c_str());
    return ok;
}

}
}

