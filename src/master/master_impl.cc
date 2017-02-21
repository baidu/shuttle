#include "master_impl.h"

#include <string>
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

DECLARE_string(galaxy_address);
DECLARE_string(nexus_root_path);
DECLARE_string(master_port);
DECLARE_string(master_lock_path);
DECLARE_string(master_path);
DECLARE_string(nexus_server_list);
DECLARE_string(jobdata_header);
DECLARE_int32(gc_interval);
DECLARE_int32(backup_interval);
DECLARE_bool(recovery);
DECLARE_bool(ignore_ins_error);
DECLARE_bool(skip_history);
DECLARE_string(galaxy_am_path);

namespace baidu {
namespace shuttle {

MasterImpl::MasterImpl() : gc_(2) {
    srand(time(NULL));
    galaxy_sdk_ = ::baidu::galaxy::sdk::AppMaster::ConnectAppMaster(
                    FLAGS_nexus_server_list, FLAGS_galaxy_am_path);
    assert(galaxy_sdk_);
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
    LOG(INFO, "master alive, recovering");
    if (FLAGS_recovery) {
        Reload();
        LOG(INFO, "master recovered");
    }
    AcquireMasterLock();
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
    JobTracker* jobtracker = new JobTracker(this, galaxy_sdk_, job);
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
        Status status = RetractJob(job_id, kKilled);
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
        job->set_start_time(jobtracker->GetStartTime());
        job->set_finish_time(jobtracker->GetFinishTime());
        response->set_error_msg(jobtracker->GetErrorMsg());
        if (request->show_detail()) {
            jobtracker->Check(response);
            jobtracker->FillCounters(response);
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
        std::map<std::string, int64_t> counters;
        ParseJobCounters(request->counters(), &counters);

        if (request->work_mode() == kReduce) {
            status = jobtracker->FinishReduce(request->task_id(),
                                              request->attempt_id(),
                                              request->task_state(),
                                              request->error_msg(),
                                              counters);
        } else {
            status = jobtracker->FinishMap(request->task_id(),
                                           request->attempt_id(),
                                           request->task_state(),
                                           request->error_msg(),
                                           counters);
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

Status MasterImpl::RetractJob(const std::string& jobid, JobState end_state) {
    MutexLock lock(&(tracker_mu_));
    MutexLock lock2(&(dead_mu_));
    std::map<std::string, JobTracker*>::iterator it = job_trackers_.find(jobid);
    if (it == job_trackers_.end()) {
        LOG(WARNING, "retract job failed: job inexist: %s", jobid.c_str());
        return kNoSuchJob;
    }

    JobTracker* jobtracker = it->second;
    job_trackers_.erase(it);
    dead_trackers_[jobid] = jobtracker;
    return jobtracker->Kill(end_state);
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
    if (FLAGS_ignore_ins_error) {
        return;
    }
    LOG(FATAL, "master lost session with nexus, die");
    abort();
}

void MasterImpl::OnMasterLockChange(const ::galaxy::ins::sdk::WatchParam& param,
                                    ::galaxy::ins::sdk::SDKError /*err*/) {
    MasterImpl* master = static_cast<MasterImpl*>(param.context);
    master->OnLockChange(param.value);
}

void MasterImpl::OnLockChange(const std::string& /*lock_session_id*/) {
    if (FLAGS_ignore_ins_error) {
        return;
    }
    std::string self_session_id = nexus_->GetSessionID();
    std::string master_lock = FLAGS_nexus_root_path + FLAGS_master_lock_path;
    std::string locker_session;
    if (!nexus_->Get(master_lock, &locker_session, NULL)) {
        abort();
    }
    if (self_session_id != locker_session) {
        LOG(WARNING, "master lost lock, try again.");
        AcquireMasterLock();
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
    std::set<std::string> gc_jobs;
    for (std::map<std::string, JobTracker*>::iterator it = dead_trackers_.begin();
            it != dead_trackers_.end(); ++it) {
        JobTracker* jobtracker = it->second;
        const std::string& jobid = it->first;
        int32_t interval_seconds = common::timer::now_time() - jobtracker->GetFinishTime();
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
    gc_.DelayTask(60000,
                  boost::bind(&MasterImpl::KeepGarbageCollecting, this));
}

bool MasterImpl::RemoveJobFromNexus(const std::string& jobid) {
    bool ok = nexus_->Delete(FLAGS_nexus_root_path + jobid, NULL);
    if (ok) {
        ok = nexus_->Delete(FLAGS_nexus_root_path + FLAGS_jobdata_header + jobid, NULL);
    }
    LOG(INFO, "[%s] remove job from nexus",
            ok ? "OK": "FAIL",
            jobid.c_str());
    return ok;
}

bool MasterImpl::SaveJobToNexus(JobTracker* jobtracker) {
    std::stringstream ss;
    jobtracker->GetJobDescriptor().SerializeToOstream(&ss);
    std::string compressed_str;
    snappy::Compress(ss.str().data(), ss.str().size(), &compressed_str);
    const std::string& jobid = jobtracker->GetJobId();
    const std::string& descriptor = compressed_str;
    const std::string& jobdata = SerialJobData(jobtracker);
    bool ok = nexus_->Put(FLAGS_nexus_root_path + jobid, descriptor, NULL);
    if (ok) {
        ok = nexus_->Put(FLAGS_nexus_root_path + FLAGS_jobdata_header + jobid, jobdata, NULL);
    }
    LOG(INFO, "[%s] job persistence: %s, desc:%d bytes, data: %d bytes",
            ok ? "OK": "FAIL",
            jobid.c_str(), descriptor.size(), jobdata.size());
    return ok;
}

void MasterImpl::KeepDataPersistence() {
    // TODO Maybe do diff here to reduce pressure
    {
        MutexLock lock(&tracker_mu_);
        for (std::map<std::string, JobTracker*>::iterator it = job_trackers_.begin();
                it != job_trackers_.end(); ++it) {
            SaveJobToNexus(it->second);
        }
    }

    {
        MutexLock lock(&dead_mu_);
        for (std::map<std::string, JobTracker*>::iterator it = dead_trackers_.begin();
             it != dead_trackers_.end(); ++it) {
            if (saved_dead_jobs_.find(it->first) == saved_dead_jobs_.end()) {
                if (SaveJobToNexus(it->second)) {
                    saved_dead_jobs_.insert(it->first);
                }
            }
        }
    }

    gc_.DelayTask(FLAGS_backup_interval, boost::bind(&MasterImpl::KeepDataPersistence, this));
}

void MasterImpl::Reload() {
    std::string jobid;
    JobDescriptor job;
    JobState state;
    std::vector<AllocateItem> history;
    std::vector<ResourceItem> resources;
    int32_t start_time;
    int32_t finish_time;
    while (GetJobDescFromNexus(jobid, job)) {
        if (FLAGS_skip_history && state != kRunning) {
            continue;
        }
        if (!GetJobInfoFromNexus(jobid, state, history, resources, start_time, finish_time)) {
            continue;
        }
        JobTracker* jobtracker = new JobTracker(this, galaxy_sdk_, job);
        bool load_succ = jobtracker->Load(jobid, state, history, resources, start_time, finish_time);
        if (load_succ) {
            if (jobtracker->GetState() == kRunning) {
                job_trackers_[jobid] = jobtracker;
            } else {
                dead_trackers_[jobid] = jobtracker;
            }
        }
        history.clear();
        resources.clear();
    }
    gc_.AddTask(boost::bind(&MasterImpl::KeepDataPersistence, this));
}

bool MasterImpl::GetJobDescFromNexus(std::string& jobid, JobDescriptor& job) {
    static ::galaxy::ins::sdk::ScanResult* result = nexus_->Scan(
            FLAGS_nexus_root_path + "job_", FLAGS_nexus_root_path + "job`");
    if (result == NULL) {
        return false;
    }
    if (result->Done()) {
        delete result;
        result = NULL;
        return false;
    }
    jobid = result->Key();
    if (jobid.size() > FLAGS_nexus_root_path.size()) {
        jobid = jobid.substr(FLAGS_nexus_root_path.size());
    }
    std::string uncompressed_str;
    snappy::Uncompress(result->Value().data(), result->Value().size(), &uncompressed_str);
    std::stringstream job_ss(uncompressed_str);
    job.ParseFromIstream(&job_ss);
    result->Next();
    return true;
}

bool MasterImpl::GetJobInfoFromNexus(const std::string& jobid, JobState& state,
                                     std::vector<AllocateItem>& history,
                                     std::vector<ResourceItem>& resources,
                                     int32_t& start_time,
                                     int32_t& finish_time) {
    std::string data_str;
    bool ok = nexus_->Get(FLAGS_nexus_root_path + FLAGS_jobdata_header + jobid, &data_str, NULL);
    if (ok) {
        ParseJobData(data_str, state, history, resources, start_time, finish_time);
    }
    return ok;
}

void MasterImpl::ParseJobData(const std::string& history_str, JobState& state,
                              std::vector<AllocateItem>& history,
                              std::vector<ResourceItem>& resources,
                              int32_t& start_time,
                              int32_t& finish_time) {
    JobCollection jc;
    std::string uncompressed_str;
    snappy::Uncompress(history_str.data(), history_str.size(), &uncompressed_str);
    std::stringstream ss(uncompressed_str);
    jc.ParseFromIstream(&ss);
    state = jc.state();
    start_time = jc.start_time();
    finish_time = jc.finish_time();
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
    int i = 0;
    ::google::protobuf::RepeatedPtrField< InputInfo >::const_iterator it2;
    for (it2 = jc.inputs().begin(); it2 != jc.inputs().end(); ++it2) {
        ResourceItem item;
        item.no = i++;
        item.attempt = 0;
        item.status = kResPending;
        item.allocated = 0;
        item.input_file = it2->input_file();
        item.offset = it2->offset();
        item.size = it2->size();
        resources.push_back(item);
    }
}

std::string MasterImpl::SerialJobData(JobTracker* const jobtracker) {
    JobCollection jc;
    jc.set_state(jobtracker->GetState());
    jc.set_start_time(jobtracker->GetStartTime());
    jc.set_finish_time(jobtracker->GetFinishTime());
    const std::vector<AllocateItem>& history = jobtracker->HistoryForDump();
    const std::vector<ResourceItem>& resources = jobtracker->InputDataForDump();
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
    for (std::vector<ResourceItem>::const_iterator it = resources.begin();
            it != resources.end(); ++it) {
        InputInfo* input = jc.add_inputs();
        input->set_input_file(it->input_file);
        input->set_offset(it->offset);
        input->set_size(it->size);
    }
    LOG(DEBUG, "jc.job_size(): %d", jc.jobs_size());
    std::stringstream ss;
    jc.SerializeToOstream(&ss);
    std::string compressed_str;
    snappy::Compress(ss.str().data(), ss.str().size(), &compressed_str);
    return compressed_str;
}

void MasterImpl::ParseJobCounters(const google::protobuf::RepeatedPtrField<baidu::shuttle::TaskCounter>& rpc_counters,
                                  std::map<std::string, int64_t>* counters) {
    assert(counters);
    google::protobuf::RepeatedPtrField<baidu::shuttle::TaskCounter>::const_iterator it;
    for (it = rpc_counters.begin(); it != rpc_counters.end(); it++) {
        (*counters)[it->key()] = it->value();
    }
}

}
}

