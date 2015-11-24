#include "master_impl.h"

#include <string>
#include <sstream>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <sys/utsname.h>
#include <gflags/gflags.h>
#include <boost/bind.hpp>

#include "logging.h"

DECLARE_string(galaxy_address);
DECLARE_string(nexus_root_path);
DECLARE_string(master_port);
DECLARE_string(master_lock_path);
DECLARE_string(master_path);
DECLARE_string(nexus_server_list);
DECLARE_string(history_header);
DECLARE_int32(gc_interval);
DECLARE_int32(backup_interval);
DECLARE_bool(recovery);

namespace baidu {
namespace shuttle {

MasterImpl::MasterImpl() {
    srand(time(NULL));
    galaxy_sdk_ = ::baidu::galaxy::Galaxy::ConnectGalaxy(FLAGS_galaxy_address);
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
    delete galaxy_sdk_;
    delete nexus_;
}

void MasterImpl::Init() {
    AcquireMasterLock();
    LOG(INFO, "master alive, recovering");
    if (FLAGS_recovery) {
        // TODO Uncomment this function until it's fully implemented
        Reload();
        LOG(INFO, "master recovered");
    }
}

void MasterImpl::SubmitJob(::google::protobuf::RpcController* /*controller*/,
                           const ::baidu::shuttle::SubmitJobRequest* request,
                           ::baidu::shuttle::SubmitJobResponse* response,
                           ::google::protobuf::Closure* done) {
    const JobDescriptor& job = request->job();
    LOG(INFO, "use dfs user: %s", job.input_dfs().user().c_str());
    LOG(INFO, "use output dfs user: %s", job.output_dfs().user().c_str());
    LOG(INFO, "pipe style: %s", PipeStyle_Name(job.pipe_style()).c_str());
    LOG(INFO, "=== job details ===");
    LOG(INFO, "%s", job.DebugString().c_str());
    LOG(INFO, "==== end of job details ==");
    JobTracker* jobtracker = new JobTracker(this, galaxy_sdk_, job, false);
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
    static const char* galaxy_priority[] = {
        "kMonitor",
        "kOnline",
        "kOffline",
        "kBestEffort"
    };
    const std::string& job_id = request->jobid();
    int map_capacity = -1, reduce_capacity = -1;
    if (request->has_map_capacity()) {
        map_capacity = request->map_capacity();
    }
    if (request->has_reduce_capacity()) {
        reduce_capacity = request->reduce_capacity();
    }
    std::string priority = request->has_priority() ? galaxy_priority[request->priority()] : "";
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        Status status = jobtracker->Update(priority, map_capacity, reduce_capacity);
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
        Status status = RetractJob(job_id);
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
            JobOverview* job = response->add_jobs();
            job->mutable_desc()->CopyFrom(it->second->GetJobDescriptor());
            job->set_jobid(it->first);
            job->set_state(it->second->GetState());
            job->mutable_map_stat()->CopyFrom(it->second->GetMapStatistics());
            job->mutable_reduce_stat()->CopyFrom(it->second->GetReduceStatistics());
        }
    }
    if (request->all()) {
        MutexLock lock2(&(dead_mu_));
        for (it = dead_trackers_.begin(); it != dead_trackers_.end(); ++it) {
            JobOverview* job = response->add_jobs();
            job->mutable_desc()->CopyFrom(it->second->GetJobDescriptor());
            job->set_jobid(it->first);
            job->set_state(it->second->GetState());
            job->mutable_map_stat()->CopyFrom(it->second->GetMapStatistics());
            job->mutable_reduce_stat()->CopyFrom(it->second->GetReduceStatistics());
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
        job->mutable_map_stat()->CopyFrom(jobtracker->GetMapStatistics());
        job->mutable_reduce_stat()->CopyFrom(jobtracker->GetReduceStatistics());

        jobtracker->Check(response);
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
        Status assign_status;
        if (request->work_mode() == kReduce) {
            IdItem* resource = jobtracker->AssignReduce(request->endpoint(), &assign_status);
            response->set_status(assign_status);
            if (resource == NULL) {
                done->Run();
                return;
            }

            TaskInfo* task = response->mutable_task();
            task->set_task_id(resource->no);
            task->set_attempt_id(resource->attempt);
            task->mutable_job()->CopyFrom(jobtracker->GetJobDescriptor());
            delete resource;
        } else {
            ResourceItem* resource = jobtracker->AssignMap(request->endpoint(), &assign_status);
            response->set_status(assign_status);
            if (resource == NULL) {
                done->Run();
                return;
            }

            TaskInfo* task = response->mutable_task();
            task->set_task_id(resource->no);
            task->set_attempt_id(resource->attempt);
            TaskInput* input = task->mutable_input();
            input->set_input_file(resource->input_file);
            input->set_input_offset(resource->offset);
            input->set_input_size(resource->size);
            task->mutable_job()->CopyFrom(jobtracker->GetJobDescriptor());
            delete resource;
        }
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
        Status status = kOk;
        if (request->work_mode() == kReduce) {
            status = jobtracker->FinishReduce(request->task_id(),
                                              request->attempt_id(),
                                              request->task_state());
        } else {
            status = jobtracker->FinishMap(request->task_id(),
                                           request->attempt_id(),
                                           request->task_state());
        }
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

Status MasterImpl::RetractJob(const std::string& jobid) {
    MutexLock lock(&(tracker_mu_));
    MutexLock lock2(&(dead_mu_));
    std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(jobid);
    if (it == job_trackers_.end()) {
        LOG(WARNING, "retract job failed: job inexist: %s", jobid.c_str());
    }

    JobTracker* jobtracker = it->second;
    job_trackers_.erase(it);
    dead_trackers_[jobid] = jobtracker;
    return jobtracker->Kill();
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

void MasterImpl::KeepGarbageCollecting() {
    MutexLock lock(&(dead_mu_));
    for (std::map<std::string, JobTracker*>::iterator it = dead_trackers_.begin();
            it != dead_trackers_.end(); ++it) {
        LOG(INFO, "[gc] remove dead job trackers: %s", it->second->GetJobId().c_str());
        delete it->second;
    }
    dead_trackers_.clear();
    gc_.DelayTask(FLAGS_gc_interval * 1000,
                  boost::bind(&MasterImpl::KeepGarbageCollecting, this));
}

void MasterImpl::KeepDataPersistence() {
    // TODO Maybe do diff here to reduce pressure
    {
        MutexLock lock(&tracker_mu_);
        for (std::map<std::string, JobTracker*>::iterator it = job_trackers_.begin();
                it != job_trackers_.end(); ++it) {
            std::stringstream ss;
            it->second->GetJobDescriptor().SerializeToOstream(&ss);
            const std::string& jobid = it->second->GetJobId();
            const std::string& descriptor = ss.str();
            const std::string& history = SerialHistory(it->second->DataForDump());
            nexus_->Put(FLAGS_nexus_root_path + jobid, descriptor, NULL);
            nexus_->Put(FLAGS_nexus_root_path + FLAGS_history_header + jobid, history, NULL);
            LOG(DEBUG, "running job persistence: %s, desc:%d bytes, history: %d bytes",
                       jobid.c_str(), descriptor.size(), history.size());
        }
    }
    MutexLock lock(&dead_mu_);
    for (std::map<std::string, JobTracker*>::iterator it = dead_trackers_.begin();
            it != dead_trackers_.end(); ++it) {
        std::stringstream ss;
        it->second->GetJobDescriptor().SerializeToOstream(&ss);
        const std::string& jobid = it->second->GetJobId();
        const std::string& descriptor = ss.str();
        const std::string& history = SerialHistory(it->second->DataForDump());
        nexus_->Put(FLAGS_nexus_root_path + jobid, descriptor, NULL);
        nexus_->Put(FLAGS_nexus_root_path + FLAGS_history_header + jobid, history, NULL);
        LOG(DEBUG, "finished job persistence: %s, desc:%d bytes, history: %d bytes",
            jobid.c_str(), descriptor.size(), history.size());
    }
    gc_.DelayTask(FLAGS_backup_interval, boost::bind(&MasterImpl::KeepDataPersistence, this));
}

void MasterImpl::Reload() {
    JobDescriptor job;
    std::vector<AllocateItem> history;
    std::string jobid;
    while (GetJobInfoFromNexus(jobid, job, history)) {
        JobTracker* jobtracker = new JobTracker(this, galaxy_sdk_, job, true);
        jobtracker->Load(jobid, history);
        if (jobtracker->GetState() == kRunning) {
            job_trackers_[jobid] = jobtracker;
        } else {
            dead_trackers_[jobid] = jobtracker;
        }
        history.clear();
    }
    gc_.AddTask(boost::bind(&MasterImpl::KeepDataPersistence, this));
}

bool MasterImpl::GetJobInfoFromNexus(std::string& jobid, JobDescriptor& job,
                                     std::vector<AllocateItem>& history) {
    static ::galaxy::ins::sdk::ScanResult* result = nexus_->Scan(
            FLAGS_nexus_root_path + "job_", FLAGS_nexus_root_path + "job`");
    if (result->Done()) {
        return false;
    }
    jobid = result->Key();
    if (jobid.size() > FLAGS_nexus_root_path.size()) {
        jobid = jobid.substr(FLAGS_nexus_root_path.size());
    }
    std::stringstream job_ss(result->Value());
    job.ParseFromIstream(&job_ss);
    std::string history_str;
    if (nexus_->Get(FLAGS_nexus_root_path + FLAGS_history_header + jobid, &history_str, NULL)) {
        ParseHistory(history_str, history);
    }
    result->Next();
    return true;
}

void MasterImpl::ParseHistory(const std::string& history_str,
                              std::vector<AllocateItem>& history) {
    JobCollection jc;
    std::stringstream ss(history_str);
    jc.ParseFromIstream(&ss);
    ::google::protobuf::RepeatedPtrField< JobAllocation >::const_iterator it;
    for (it = jc.jobs().begin(); it != jc.jobs().end(); ++it) {
        AllocateItem item;
        item.resource_no = it->resource_no();
        item.attempt = it->attempt();
        item.endpoint = it->endpoint();
        item.state = it->state();
        item.alloc_time = it->alloc_time();
        item.period = it->period();
        item.is_map = it->is_map();
        history.push_back(item);
    }
}

std::string MasterImpl::SerialHistory(const std::vector<AllocateItem>& history) {
    JobCollection jc;
    for (std::vector<AllocateItem>::const_iterator it = history.begin();
            it != history.end(); ++it) {
        JobAllocation* job = jc.add_jobs();
        job->set_resource_no(it->resource_no);
        job->set_attempt(it->attempt);
        job->set_endpoint(it->endpoint);
        job->set_state(it->state);
        job->set_alloc_time(it->alloc_time);
        job->set_period(it->period);
        job->set_is_map(it->is_map);
    }
    LOG(DEBUG, "jc.job_size(): %d", jc.jobs_size());
    std::stringstream ss;
    jc.SerializeToOstream(&ss);
    return ss.str();
}

}
}

