#include "master_impl.h"

#include <string>
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
DECLARE_int32(gc_interval);

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
    Reload();
    LOG(INFO, "master recovered");
}

void MasterImpl::SubmitJob(::google::protobuf::RpcController* /*controller*/,
                           const ::baidu::shuttle::SubmitJobRequest* request,
                           ::baidu::shuttle::SubmitJobResponse* response,
                           ::google::protobuf::Closure* done) {
    const JobDescriptor& job = request->job();
    JobTracker* jobtracker = new JobTracker(this, galaxy_sdk_, job);
    Status status = jobtracker->Start();
    const std::string& job_id = jobtracker->GetJobId();
    {
        MutexLock lock(&(tracker_mu_));
        job_trackers_[job_id] = jobtracker;
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
    std::string priority = galaxy_priority[request->priority()];
    JobTracker* jobtracker = NULL;
    {
        MutexLock lock(&(tracker_mu_));
        std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(job_id);
        if (it != job_trackers_.end()) {
            jobtracker = it->second;
        }
    }
    if (jobtracker != NULL) {
        Status status = jobtracker->Update(priority,
                                           request->map_capacity(),
                                           request->reduce_capacity());
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
                          const ::baidu::shuttle::ListJobsRequest* /*request*/,
                          ::baidu::shuttle::ListJobsResponse* response,
                          ::google::protobuf::Closure* done) {
    std::map<std::string, JobTracker*>::iterator it;
    MutexLock lock1(&(tracker_mu_));
    MutexLock lock2(&(dead_mu_));
    for (it = job_trackers_.begin(); it != job_trackers_.end(); ++it) {
        JobOverview* job = response->add_jobs();
        job->mutable_desc()->CopyFrom(it->second->GetJobDescriptor());
        job->set_jobid(it->first);
        job->set_state(it->second->GetState());
        job->mutable_map_stat()->CopyFrom(it->second->GetMapStatistics());
        job->mutable_reduce_stat()->CopyFrom(it->second->GetReduceStatistics());
    }
    for (it = dead_trackers_.begin(); it != dead_trackers_.end(); ++it) {
        JobOverview* job = response->add_jobs();
        job->mutable_desc()->CopyFrom(it->second->GetJobDescriptor());
        job->set_jobid(it->first);
        job->set_state(it->second->GetState());
        job->mutable_map_stat()->CopyFrom(it->second->GetMapStatistics());
        job->mutable_reduce_stat()->CopyFrom(it->second->GetReduceStatistics());
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
    if (jobtracker == NULL) {
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

        ::google::protobuf::internal::RepeatedPtrFieldBackInsertIterator<TaskOverview>
            back_it = ::google::protobuf::RepeatedFieldBackInserter(response->mutable_tasks());
        jobtracker->Check(back_it);
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
        if (request->work_mode() == kReduce) {
            IdItem* resource = jobtracker->AssignReduce(request->endpoint());
            if (resource == NULL) {
                response->set_status(kNoMore);
                done->Run();
                return;
            }

            TaskInfo* task = response->mutable_task();
            task->set_task_id(resource->no);
            task->set_attempt_id(resource->attempt);
            task->mutable_job()->CopyFrom(jobtracker->GetJobDescriptor());
            delete resource;
        } else {
            ResourceItem* resource = jobtracker->AssignMap(request->endpoint());
            if (resource == NULL) {
                response->set_status(kNoMore);
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

        response->set_status(kOk); 
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

void MasterImpl::Reload() {
    // TODO Reload saved meta data
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
        delete it->second;
    }
    dead_trackers_.clear();
    gc_.DelayTask(FLAGS_gc_interval * 1000,
                  boost::bind(&MasterImpl::KeepGarbageCollecting, this));
}

}
}

