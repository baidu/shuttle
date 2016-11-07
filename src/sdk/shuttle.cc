#include "shuttle.h"

#include <iterator>
#include <algorithm>

#include "proto/master.pb.h"
#include "common/rpc_client.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

const static int sDefaultRpcTimeout = 1200;

class ShuttleImpl : public Shuttle {
public:
    ShuttleImpl(const std::string& master_addr);
    virtual ~ShuttleImpl();
    bool SubmitJob(const sdk::JobDescription& job_desc, std::string& job_id);
    bool UpdateJob(const std::string& job_id,
                   const std::map<int32_t, int32_t>& new_capacities);
    bool KillJob(const std::string& job_id);
    bool KillTask(const std::string& job_id, int node,
                  int task_id, int attempt_id);
    bool ShowJob(const std::string& job_id, 
                 sdk::JobInstance& job,
                 std::vector<sdk::TaskInstance>& tasks,
                 bool display_all);
    bool ListJobs(std::vector<sdk::JobInstance>& jobs,
                  bool display_all);
    void SetRpcTimeout(int second);
private:
    void ConvertJobInstance(const JobOverview& desc, sdk::JobInstance& job);

private:
    std::string master_addr_;
    Master_Stub* master_stub_;
    RpcClient rpc_client_;
    int rpc_timeout_;
};

Shuttle* Shuttle::Connect(const std::string& master_addr) {
    return new ShuttleImpl(master_addr);
}

ShuttleImpl::ShuttleImpl(const std::string& master_addr) {
    master_addr_ = master_addr;
    rpc_client_.GetStub(master_addr_, &master_stub_);
    rpc_timeout_ = sDefaultRpcTimeout;
}

ShuttleImpl::~ShuttleImpl() {
    delete master_stub_;
}

void ShuttleImpl::SetRpcTimeout(int rpc_timeout) {
    rpc_timeout_ = rpc_timeout;
}

bool ShuttleImpl::SubmitJob(const sdk::JobDescription& job_desc, std::string& job_id) {
    ::baidu::shuttle::SubmitJobRequest request;
    ::baidu::shuttle::SubmitJobResponse response;
    ::baidu::shuttle::JobDescriptor* job = request.mutable_job();
    job->set_name(job_desc.name);
    std::copy(job_desc.files.begin(), job_desc.files.end(),
              ::google::protobuf::RepeatedFieldBackInserter(job->mutable_files()));
    job->set_cache_archive(job_desc.cache_archive);
    job->set_pipe_style((PipeStyle)job_desc.pipe_style);
    job->set_split_size(job_desc.split_size);
    for (std::vector<sdk::NodeConfig>::const_iterator it = job_desc.nodes.begin();
            it != job_desc.nodes.end(); ++it) {
        const sdk::NodeConfig& cur = *it;
        NodeConfig* node = job->add_nodes();
        node->set_node(cur.node);
        node->set_type((WorkMode)cur.type);
        node->set_capacity(cur.capacity);
        node->set_total(cur.total);
        node->set_millicores(cur.millicores);
        node->set_memory(cur.memory);
        node->set_command(cur.command);
        for (std::vector<sdk::DfsInfo>::const_iterator jt = cur.inputs.begin();
                jt != cur.inputs.end(); ++jt) {
            DfsInfo* info = node->add_inputs();
            info->set_path(jt->path);
            info->set_host(jt->host);
            info->set_port(jt->port);
            info->set_user(jt->user);
            info->set_password(jt->password);
        }
        node->set_input_format((InputFormat)cur.input_format);
        {
            DfsInfo* info = node->mutable_output();
            info->set_path(cur.output.path);
            info->set_host(cur.output.host);
            info->set_port(cur.output.port);
            info->set_user(cur.output.user);
            info->set_password(cur.output.password);
        }
        node->set_output_format((OutputFormat)cur.output_format);
        node->set_partition((Partition)cur.partition);
        node->set_key_separator(cur.key_separator);
        node->set_key_fields_num(cur.key_fields_num);
        node->set_partition_fields_num(cur.partition_fields_num);
        node->set_allow_duplicates(cur.allow_duplicates);
        node->set_retry(cur.retry);
        node->set_combiner(cur.combiner);
        node->set_check_counters(cur.check_counters);
        node->set_ignore_failures(cur.ignore_failures);
        node->set_decompress_input(cur.decompress_input);
        node->set_compress_output(cur.compress_output);
        for (std::vector<std::string>::const_iterator it = cur.cmdenvs.begin();
                it != cur.cmdenvs.end(); ++it) {
            node->add_cmdenvs(*it);
        }
    }
    for (std::vector< std::vector<int32_t> >::const_iterator it = job_desc.map.begin();
            it != job_desc.map.end(); ++it) {
        JobDescriptor_NodeNeigbor* cur = job->add_map();
        for (std::vector<int32_t>::const_iterator jt = it->begin(); jt != it->end(); ++jt) {
            cur->add_next(*jt);
        }
    }

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::SubmitJob,
                                      &request, &response, rpc_timeout_, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() == kNoMore) {
        LOG(WARNING, "input not exists");
        return false;
    } else if (response.status() == kWriteFileFail) {
        LOG(WARNING, "output file exists");
        return false;
    }
    job_id = response.jobid();
    return true;
}

bool ShuttleImpl::UpdateJob(const std::string& job_id,
                            const std::map<int32_t, int32_t>& new_capacities) {
    ::baidu::shuttle::UpdateJobRequest request;
    ::baidu::shuttle::UpdateJobResponse response;
    request.set_jobid(job_id);
    for (std::map<int32_t, int32_t>::const_iterator it = new_capacities.begin();
            it != new_capacities.end(); ++it) {
        ::baidu::shuttle::UpdateJobRequest_UpdatedNode* cur = request.add_capacities();
        cur->set_node(it->first);
        cur->set_capacity(it->second);
    }

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::UpdateJob,
                                      &request, &response, rpc_timeout_, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        LOG(WARNING, "update status: %s", Status_Name(response.status()).c_str());
    }
    return response.status() == kOk;
}

bool ShuttleImpl::KillJob(const std::string& job_id) {
    ::baidu::shuttle::KillJobRequest request;
    ::baidu::shuttle::KillJobResponse response;
    request.set_jobid(job_id);

    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::KillJob,
                                      &request, &response, rpc_timeout_, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    return response.status() == kOk;
}

bool ShuttleImpl::KillTask(const std::string& job_id, int node,
                           int task_id, int attempt_id) {
    ::baidu::shuttle::FinishTaskRequest request;
    ::baidu::shuttle::FinishTaskResponse response;
    request.set_jobid(job_id);
    request.set_node(node);
    request.set_task_id(task_id);
    request.set_attempt_id(attempt_id);
    request.set_task_state(kTaskKilled);
    request.set_endpoint("0.0.0.0");
    bool ok = rpc_client_.SendRequest(master_stub_, &Master_Stub::FinishTask,
                                      &request, &response, rpc_timeout_, 1);
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
                                      &request, &response, rpc_timeout_, 1);
    if (!ok) {
        LOG(WARNING, "failed to rpc: %s", master_addr_.c_str());
        return false;
    }
    if (response.status() != kOk) {
        return false;
    }

    ConvertJobInstance(response.job(), job);

    ::google::protobuf::RepeatedPtrField<TaskOverview>::const_iterator it;
    for (it = response.tasks().begin(); it != response.tasks().end(); ++it) {
        sdk::TaskInstance task;
        const TaskInfo& info = it->info();
        task.job_id = job.jobid;
        task.node = info.node();
        task.task_id = info.task_id();
        task.attempt_id = info.attempt_id();
        task.input_file = info.input().input_file();
        task.state = (sdk::TaskState)it->state();
        task.minion_addr = it->minion_addr();
        task.progress = it->progress();
        task.start_time = it->start_time();
        task.finish_time = it->finish_time();
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
                                      &request, &response, rpc_timeout_, 1);
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
        ConvertJobInstance(*it, job);
        jobs.push_back(job);
    }
    return true;
}

void ShuttleImpl::ConvertJobInstance(const JobOverview& joboverview,
                                     sdk::JobInstance& job) {
    const JobDescriptor& desc = joboverview.desc();
    job.desc.name = desc.name();
    job.desc.files.resize(desc.files().size());
    std::copy(desc.files().begin(), desc.files().end(), job.desc.files.begin());
    job.desc.cache_archive = desc.cache_archive();
    job.desc.pipe_style = (sdk::PipeStyle)desc.pipe_style();
    job.desc.split_size = desc.split_size();
    ::google::protobuf::RepeatedPtrField< ::baidu::shuttle::NodeConfig >::const_iterator it;
    for (it = desc.nodes().begin(); it != desc.nodes().end(); ++it) {
        job.desc.nodes.push_back(sdk::NodeConfig());
        sdk::NodeConfig& cur = job.desc.nodes.back();
        cur.node = it->node();
        cur.type = (sdk::WorkMode)it->type();
        cur.capacity = it->capacity();
        cur.total = it->total();
        cur.millicores = it->millicores();
        cur.memory = it->memory();
        cur.command = it->command();
        ::google::protobuf::RepeatedPtrField<DfsInfo>::const_iterator jt;
        for (jt = it->inputs().begin(); jt != it->inputs().end(); ++jt) {
            sdk::DfsInfo info;
            info.path = jt->path();
            info.host = jt->host();
            info.port = jt->port();
            info.user = jt->user();
            info.password = jt->password();
            cur.inputs.push_back(info);
        }
        cur.input_format = (sdk::InputFormat)it->input_format();
        cur.output.path = it->output().path();
        cur.output.host = it->output().host();
        cur.output.port = it->output().port();
        cur.output.user = it->output().user();
        cur.output.password = it->output().password();
        cur.output_format = (sdk::OutputFormat)it->output_format();
        cur.partition = (sdk::PartitionMethod)it->partition();
        cur.key_separator = it->key_separator();
        cur.key_fields_num = it->key_fields_num();
        cur.partition_fields_num = it->partition_fields_num();
        cur.allow_duplicates = it->allow_duplicates();
        cur.retry = it->retry();
        cur.combiner = it->combiner();
        cur.check_counters = it->check_counters();
        cur.ignore_failures = it->ignore_failures();
        cur.decompress_input = it->decompress_input();
        cur.compress_output = it->compress_output();
        ::google::protobuf::RepeatedPtrField<std::string>::const_iterator st;
        for (st = it->cmdenvs().begin(); st != it->cmdenvs().end(); ++st) {
            cur.cmdenvs.push_back(*st);
        }
    }

    job.jobid = joboverview.jobid();
    job.state = (sdk::JobState)joboverview.state();
    ::google::protobuf::RepeatedPtrField<TaskStatistics>::const_iterator stat;
    for (stat = joboverview.stats().begin(); stat != joboverview.stats().end(); ++stat) {
        sdk::TaskStatistics cur;
        cur.total = stat->total();
        cur.pending = stat->pending();
        cur.running = stat->running();
        cur.failed = stat->failed();
        cur.killed = stat->killed();
        cur.completed = stat->completed();
        job.stats.push_back(cur);
    }
    job.start_time = joboverview.start_time();
    job.finish_time = joboverview.finish_time();
}

} // namespace shuttle
} // namespace baidu

