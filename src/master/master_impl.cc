#include "master_impl.h"

#include <string>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <sys/utsname.h>
#include <gflags/gflags.h>

#include "logging.h"

DECLARE_string(galaxy_address);
DECLARE_string(master_port);
DECLARE_string(master_lock_path);
DECLARE_string(master_key);
DECLARE_string(nexus_server_list);

namespace baidu {
namespace shuttle {

MasterImpl::MasterImpl() {
    srand(time(NULL));
    galaxy_sdk_ = ::baidu::galaxy::Galaxy::ConnectGalaxy(FLAGS_galaxy_address);
    nexus_ = new ::galaxy::ins::sdk::InsSDK(FLAGS_nexus_server_list);
}

MasterImpl::~MasterImpl() {
    delete galaxy_sdk_;
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
    JobTracker* jobtracker = new JobTracker(galaxy_sdk_, job);
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
        Status status = jobtracker->Kill();
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
    MutexLock lock(&(tracker_mu_));
    std::map<std::string, JobTracker*>::iterator it;
    for (it = job_trackers_.begin(); it != job_trackers_.end(); ++it) {
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
    if (jobtracker != NULL) {
        response->set_status(kOk);
        JobOverview* job = response->mutable_job();
        job->mutable_desc()->CopyFrom(jobtracker->GetJobDescriptor());
        job->set_jobid(job_id);
        job->set_state(jobtracker->GetState());
        job->mutable_map_stat()->CopyFrom(jobtracker->GetMapStatistics());
        job->mutable_reduce_stat()->CopyFrom(jobtracker->GetReduceStatistics());
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
        ResourceItem* resource = jobtracker->Assign(request->endpoint());
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

        response->set_status(kOk); 
        delete resource;
    } else {
        LOG(WARNING, "assign task failed: job inexist: %s", job_id.c_str());
        response->set_status(kNoSuchJob);
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
        Status status = jobtracker->FinishTask(request->task_id(),
                                               request->attempt_id(),
                                               request->task_state());
        response->set_status(status);
    } else {
        LOG(WARNING, "finish task failed: job inexist: %s", job_id.c_str());
        response->set_status(kNoSuchJob);
    }
    done->Run();
}

void MasterImpl::AcquireMasterLock() {
    std::string master_lock = FLAGS_master_lock_path;
    ::galaxy::ins::sdk::SDKError err;
    nexus_->RegisterSessionTimeout(&OnMasterSessionTimeout, this);
    bool ret = nexus_->Lock(master_lock, &err);
    assert(ret && err == ::galaxy::ins::sdk::kOK);
    std::string master_key = FLAGS_master_key;
    std::string master_endpoint = SelfEndpoint();
    ret = nexus_->Put(master_key, master_endpoint, &err);
    assert(ret && err == ::galaxy::ins::sdk::kOK);
    // TODO Maybe watch own lock?
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

std::string MasterImpl::SelfEndpoint() {
    std::string hostname = "";
    struct utsname buf;
    if (0 != uname(&buf)) {
        *buf.nodename = '\0';
    }
    hostname = buf.nodename;
    return hostname + ":" + FLAGS_master_port;
}

}
}

