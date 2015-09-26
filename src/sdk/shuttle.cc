#include "shuttle.h"

#include <iterator>
#include <algorithm>

#include "proto/app_master.pb.h"
#include "common/rpc_client.h"

namespace baidu {
namespace shuttle {

class ShuttleImpl : public Shuttle {
public:
    ShuttleImpl(const std::string& master_addr);
    virtual ~ShuttleImpl();
    bool SubmitJob(const sdk::JobDescription& job_desc, std::string* job_id);
    bool UpdateJob(const std::string& job_id, 
                   const sdk::JobDescription& job_desc);
    bool KillJob(const std::string& job_id);
    bool ShowJob(const std::string& job_id, 
                 sdk::JobInstance* job,
                 std::vector<sdk::TaskInstance>* tasks);
    bool ListJobs(std::vector<sdk::JobInstance>* jobs);
private:
    std::string master_addr_;
    Master_Stub* master_stub_;
    RpcClient rpc_client_;
};

Shuttle* Shuttle::Connect(const std::string& master_addr) {
    return new ShuttleImpl(master_addr);
}

ShuttleImpl::ShuttleImpl(const std::string& master_addr) {
    master_addr_ = master_addr;
    rpc_client_.GetStub(master_addr_, &master_stub_);
}

ShuttleImpl::~ShuttleImpl() {
    delete master_stub_;
}

bool ShuttleImpl::SubmitJob(const sdk::JobDescription& job_desc, std::string* job_id) {
    ::baidu::shuttle::SubmitJobRequest request;
    ::baidu::shuttle::SubmitJobResponse response;
    ::baidu::shuttle::JobDescriptor* job = request.mutable_job();
    job->set_name(job_desc.name);
    job->set_user(job_desc.user);
    job->set_priority((JobPriority)job_desc.priority);
    job->set_map_capacity(job_desc.map_capacity);
    job->set_reduce_capacity(job_desc.reduce_capacity);
    job->set_millicores(job_desc.millicores);
    job->set_memory(job_desc.memory);
    for (std::vector<std::string>::const_iterator it = job_desc.inputs.begin();
            it != job_desc.inputs.end(); ++it) {
        job->add_inputs(*it);
    }
    job->set_output(job_desc.output);
    for (std::vector<std::string>::const_iterator it = job_desc.files.begin();
            it != job_desc.files.end(); ++it) {
        job->add_files(*it);
    }
    job->set_map_command(job_desc.map_command);
    job->set_reduce_command(job_desc.reduce_command);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::SubmitJob,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }
    if (job_id != NULL) {
        *job_id = response.jobid();
    }
    return true;
}

bool ShuttleImpl::UpdateJob(const std::string& job_id, 
                            const sdk::JobDescription& job_desc) {
    ::baidu::shuttle::UpdateJobRequest request;
    ::baidu::shuttle::UpdateJobResponse response;
    request.set_jobid(job_id);
    request.set_priority((JobPriority)job_desc.priority);
    request.set_map_capacity(job_desc.map_capacity);
    request.set_reduce_capacity(job_desc.reduce_capacity);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::UpdateJob,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    return response.status() == kOk;
}

bool ShuttleImpl::KillJob(const std::string& job_id) {
    ::baidu::shuttle::KillJobRequest request;
    ::baidu::shuttle::KillJobResponse response;
    request.set_jobid(job_id);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::KillJob,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    return response.status() == kOk;
}

bool ShuttleImpl::ShowJob(const std::string& job_id, 
                          sdk::JobInstance* job,
                          std::vector<sdk::TaskInstance>* tasks) {
    ::baidu::shuttle::ShowJobRequest request;
    ::baidu::shuttle::ShowJobResponse response;
    request.set_jobid(job_id);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::ShowJob,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }
    if (job != NULL) {
        const JobOverview& joboverview = response.job();
        const JobDescriptor& desc = joboverview.desc();
        job->desc.name = desc.name();
        job->desc.user = desc.user();
        job->desc.priority = (sdk::JobPriority)desc.priority();
        job->desc.map_capacity = desc.map_capacity();
        job->desc.reduce_capacity = desc.reduce_capacity();
        job->desc.millicores = desc.millicores();
        job->desc.memory = desc.memory();
        std::copy(desc.inputs().begin(), desc.inputs().end(),
                  std::back_inserter(job->desc.inputs));
        job->desc.output = desc.output();
        std::copy(desc.files().begin(), desc.files().end(),
                  std::back_inserter(job->desc.files));
        job->desc.map_command = desc.map_command();
        job->desc.reduce_command = desc.reduce_command();

        job->jobid = joboverview.jobid();
        job->state = (sdk::JobState)joboverview.state();

        const TaskStatistics& map_stat = joboverview.map_stat();
        job->map_stat.total = map_stat.total();
        job->map_stat.pending = map_stat.pending();
        job->map_stat.running = map_stat.running();
        job->map_stat.failed = map_stat.failed();
        job->map_stat.killed = map_stat.killed();
        job->map_stat.completed = map_stat.completed();

        const TaskStatistics& reduce_stat = joboverview.reduce_stat();
        job->reduce_stat.total = reduce_stat.total();
        job->reduce_stat.pending = reduce_stat.pending();
        job->reduce_stat.running = reduce_stat.running();
        job->reduce_stat.failed = reduce_stat.failed();
        job->reduce_stat.killed = reduce_stat.killed();
        job->reduce_stat.completed = reduce_stat.completed();
    }
    if (tasks != NULL) {
        ::google::protobuf::RepeatedPtrField<TaskOverview>::const_iterator it;
        for (it = response.tasks().begin(); it != response.tasks().end(); ++it) {
            sdk::TaskInstance task;
            const TaskInfo& info = it->info();
            task.job_id = info.task_id();
            task.attempt_id = info.attempt_id();
            task.input_file = info.input().input_file();
            task.state = (sdk::TaskState)it->state();
            task.minion_addr = it->minion_addr();
            task.progress = it->progress();
            tasks->push_back(task);
        }
    }
    return true;
}

bool ShuttleImpl::ListJobs(std::vector<sdk::JobInstance>* jobs) {
    ::baidu::shuttle::ListJobsRequest request;
    ::baidu::shuttle::ListJobsResponse response;

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::ListJobs,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }
    if (jobs != NULL) {
        ::google::protobuf::RepeatedPtrField<JobOverview>::const_iterator it;
        for (it = response.jobs().begin(); it != response.jobs().end(); ++it) {
            sdk::JobInstance job;
            const JobDescriptor& desc = it->desc();
            job.desc.name = desc.name();
            job.desc.user = desc.user();
            job.desc.priority = (sdk::JobPriority)desc.priority();
            job.desc.map_capacity = desc.map_capacity();
            job.desc.reduce_capacity = desc.reduce_capacity();
            job.desc.millicores = desc.millicores();
            job.desc.memory = desc.memory();
            std::copy(desc.inputs().begin(), desc.inputs().end(),
                      std::back_inserter(job.desc.inputs));
            job.desc.output = desc.output();
            std::copy(desc.files().begin(), desc.files().end(),
                      std::back_inserter(job.desc.files));
            job.desc.map_command = desc.map_command();
            job.desc.reduce_command = desc.reduce_command();

            job.jobid = it->jobid();
            job.state = (sdk::JobState)it->state();

            const TaskStatistics& map_stat = it->map_stat();
            job.map_stat.total = map_stat.total();
            job.map_stat.pending = map_stat.pending();
            job.map_stat.running = map_stat.running();
            job.map_stat.failed = map_stat.failed();
            job.map_stat.killed = map_stat.killed();
            job.map_stat.completed = map_stat.completed();

            const TaskStatistics& reduce_stat = it->reduce_stat();
            job.reduce_stat.total = reduce_stat.total();
            job.reduce_stat.pending = reduce_stat.pending();
            job.reduce_stat.running = reduce_stat.running();
            job.reduce_stat.failed = reduce_stat.failed();
            job.reduce_stat.killed = reduce_stat.killed();
            job.reduce_stat.completed = reduce_stat.completed();
            jobs->push_back(job);
        }
    }
    return true;
}

} //namespace shuttle
} //namespace baidu

