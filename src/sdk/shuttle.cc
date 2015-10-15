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
    bool SubmitJob(const sdk::JobDescription& job_desc, std::string& job_id);
    bool UpdateJob(const std::string& job_id,
                   const sdk::JobPriority& priority = sdk::kUndefined,
                   const int map_capacity = -1,
                   const int reduce_capacity = -1);
    bool KillJob(const std::string& job_id);
    bool ShowJob(const std::string& job_id, 
                 sdk::JobInstance& job,
                 std::vector<sdk::TaskInstance>& tasks,
                 bool display_all);
    bool ListJobs(std::vector<sdk::JobInstance>& jobs,
                  bool display_all);
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

bool ShuttleImpl::SubmitJob(const sdk::JobDescription& job_desc, std::string& job_id) {
    ::baidu::shuttle::SubmitJobRequest request;
    ::baidu::shuttle::SubmitJobResponse response;
    ::baidu::shuttle::JobDescriptor* job = request.mutable_job();
    job->set_name(job_desc.name);
    job->set_user(job_desc.user);
    job->set_priority((job_desc.priority == sdk::kUndefined) ?
            kNormal : (JobPriority)job_desc.priority);
    job->set_map_capacity(job_desc.map_capacity);
    job->set_reduce_capacity(job_desc.reduce_capacity);
    job->set_millicores(job_desc.millicores);
    job->set_memory(job_desc.memory);
    std::copy(job_desc.inputs.begin(), job_desc.inputs.end(),
              ::google::protobuf::RepeatedFieldBackInserter(job->mutable_inputs()));
    job->set_output(job_desc.output);
    std::copy(job_desc.files.begin(), job_desc.files.end(),
              ::google::protobuf::RepeatedFieldBackInserter(job->mutable_files()));
    job->set_map_command(job_desc.map_command);
    job->set_reduce_command(job_desc.reduce_command);
    job->set_partition((Partition)job_desc.partition);
    job->set_map_total(job_desc.map_total);
    job->set_reduce_total(job_desc.reduce_total);
    job->set_key_separator(job_desc.key_separator);
    job->set_key_fields_num(job_desc.key_fields_num);
    job->set_partition_fields_num(job_desc.partition_fields_num);
    job->set_job_type((job_desc.reduce_total == 0) ? kMapOnlyJob : kMapReduceJob);
    DfsInfo* input_info = job->mutable_input_dfs();
    input_info->set_host(job_desc.input_dfs.host);
    input_info->set_port(job_desc.input_dfs.port);
    input_info->set_user(job_desc.input_dfs.user);
    input_info->set_password(job_desc.input_dfs.password);
    DfsInfo* output_info = job->mutable_output_dfs();
    output_info->set_host(job_desc.output_dfs.host);
    output_info->set_port(job_desc.output_dfs.port);
    output_info->set_user(job_desc.output_dfs.user);
    output_info->set_password(job_desc.output_dfs.password);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::SubmitJob,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }
    job_id = response.jobid();
    return true;
}

bool ShuttleImpl::UpdateJob(const std::string& job_id, const sdk::JobPriority& priority,
                            const int map_capacity, const int reduce_capacity) {
    ::baidu::shuttle::UpdateJobRequest request;
    ::baidu::shuttle::UpdateJobResponse response;
    request.set_jobid(job_id);
    if (priority != sdk::kUndefined) {
        request.set_priority((JobPriority)priority);
    }
    if (map_capacity != -1) {
        request.set_map_capacity(map_capacity);
    }
    if (reduce_capacity != -1) {
        request.set_reduce_capacity(reduce_capacity);
    }

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
                          sdk::JobInstance& job,
                          std::vector<sdk::TaskInstance>& tasks,
                          bool display_all) {
    ::baidu::shuttle::ShowJobRequest request;
    ::baidu::shuttle::ShowJobResponse response;
    request.set_jobid(job_id);
    request.set_all(display_all);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::ShowJob,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }

    const JobOverview& joboverview = response.job();
    const JobDescriptor& desc = joboverview.desc();
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
    job.desc.partition = (sdk::PartitionMethod)desc.partition();
    job.desc.map_total = desc.map_total();
    job.desc.reduce_total = desc.reduce_total();
    job.desc.key_separator = desc.key_separator();
    job.desc.key_fields_num = desc.key_fields_num();
    job.desc.partition_fields_num = desc.partition_fields_num();
    job.desc.input_dfs.host = desc.input_dfs().host();
    job.desc.input_dfs.port = desc.input_dfs().port();
    job.desc.input_dfs.user = desc.input_dfs().user();
    job.desc.input_dfs.password = desc.input_dfs().password();
    job.desc.output_dfs.host = desc.output_dfs().host();
    job.desc.output_dfs.port = desc.output_dfs().port();
    job.desc.output_dfs.user = desc.output_dfs().user();
    job.desc.output_dfs.password = desc.output_dfs().password();

    job.jobid = joboverview.jobid();
    job.state = (sdk::JobState)joboverview.state();

    const TaskStatistics& map_stat = joboverview.map_stat();
    job.map_stat.total = map_stat.total();
    job.map_stat.pending = map_stat.pending();
    job.map_stat.running = map_stat.running();
    job.map_stat.failed = map_stat.failed();
    job.map_stat.killed = map_stat.killed();
    job.map_stat.completed = map_stat.completed();

    const TaskStatistics& reduce_stat = joboverview.reduce_stat();
    job.reduce_stat.total = reduce_stat.total();
    job.reduce_stat.pending = reduce_stat.pending();
    job.reduce_stat.running = reduce_stat.running();
    job.reduce_stat.failed = reduce_stat.failed();
    job.reduce_stat.killed = reduce_stat.killed();
    job.reduce_stat.completed = reduce_stat.completed();

    ::google::protobuf::RepeatedPtrField<TaskOverview>::const_iterator it;
    for (it = response.tasks().begin(); it != response.tasks().end(); ++it) {
        sdk::TaskInstance task;
        const TaskInfo& info = it->info();
        task.job_id = job.jobid;
        task.task_id = info.task_id();
        task.attempt_id = info.attempt_id();
        task.input_file = info.input().input_file();
        task.state = (sdk::TaskState)it->state();
        task.type = (sdk::TaskType)info.task_type();
        task.minion_addr = it->minion_addr();
        task.progress = it->progress();
        tasks.push_back(task);
    }
    return true;
}

bool ShuttleImpl::ListJobs(std::vector<sdk::JobInstance>& jobs,
                           bool display_all) {
    ::baidu::shuttle::ListJobsRequest request;
    ::baidu::shuttle::ListJobsResponse response;
    request.set_all(display_all);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::ListJobs,
                                      &request, &response, 2, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }
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
        job.desc.partition = (sdk::PartitionMethod)desc.partition();
        job.desc.map_total = desc.map_total();
        job.desc.reduce_total = desc.reduce_total();
        job.desc.key_separator = desc.key_separator();
        job.desc.key_fields_num = desc.key_fields_num();
        job.desc.partition_fields_num = desc.partition_fields_num();
        job.desc.input_dfs.host = desc.input_dfs().host();
        job.desc.input_dfs.port = desc.input_dfs().port();
        job.desc.input_dfs.user = desc.input_dfs().user();
        job.desc.input_dfs.password = desc.input_dfs().password();
        job.desc.output_dfs.host = desc.output_dfs().host();
        job.desc.output_dfs.port = desc.output_dfs().port();
        job.desc.output_dfs.user = desc.output_dfs().user();
        job.desc.output_dfs.password = desc.output_dfs().password();

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
        jobs.push_back(job);
    }
    return true;
}

} //namespace shuttle
} //namespace baidu

