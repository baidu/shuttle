#include "minion_impl.h"

#include <gflags/gflags.h>
#include <cstdio>
#include <cstdlib>
#include "proto/master.pb.h"
#include "logging.h"
#include "ins_sdk.h"

DECLARE_string(breakpoint);
DECLARE_string(jobid);
DECLARE_string(nexus_addr);
DECLARE_string(master_nexus_path);
DECLARE_int32(node);
DECLARE_int32(suspend_time);

namespace baidu {
namespace shuttle {

MinionImpl::MinionImpl() : running_(true), task_id_(-1), attempt_id_(-1),
        state_(kTaskUnknown), executor_(NULL) {
    executor_ = new Executor(FLAGS_jobid);
}

void MinionImpl::Query(::google::protobuf::RpcController* /*controller*/,
                       const ::baidu::shuttle::QueryRequest* /*request*/,
                       ::baidu::shuttle::QueryResponse* response,
                       ::google::protobuf::Closure* done) {
    MutexLock lock(&mu_);
    response->set_job_id(FLAGS_jobid);
    response->set_node(FLAGS_node);
    response->set_task_id(task_id_);
    response->set_attempt_id(attempt_id_);
    response->set_task_state(state_);
    done->Run();
}

void MinionImpl::CancelTask(::google::protobuf::RpcController* /*controller*/,
                            const ::baidu::shuttle::CancelTaskRequest* request,
                            ::baidu::shuttle::CancelTaskResponse* response,
                            ::google::protobuf::Closure* done){
    Status ret = kOk;
    {
        MutexLock lock(&mu_);
        if (request->job_id() != FLAGS_jobid ||
                request->node() != FLAGS_node ||
                request->task_id() != task_id_ ||
                request->attempt_id() != attempt_id_) {
            ret = kNoSuchTask;
        } else if (executor_ == NULL) {
            ret = kNoSuchTask;
        } else {
            executor_->Stop(request->task_id());
            ret = kOk;
        }
    }
    response->set_status(ret);
    done->Run();
}

void MinionImpl::SetEndpoint(const std::string& endpoint) {
    LOG(INFO, "minion bind endpoint on: %s", endpoint.c_str());
    endpoint_ = endpoint;
}

void MinionImpl::Run() {
    if (!GetMasterEndpoint()) {
        return;
    }
    CheckBreakpoint();
    Master_Stub* stub = NULL;
    if (!rpc_client_.GetStub(master_endpoint_, &stub) || stub == NULL) {
        LOG(WARNING, "fail to get stub to master");
        return;
    }
    int task_count = 0;
    while (running_) {
        LOG(INFO, "========== task %d ==========", ++task_count);

        AssignTaskRequest assign_request;
        AssignTaskResponse assign_response;
        assign_request.set_jobid(FLAGS_jobid);
        assign_request.set_node(FLAGS_node);
        assign_request.set_endpoint(endpoint_);
        LOG(INFO, "request %s task for: %s", FLAGS_jobid.c_str(), endpoint_.c_str());
        bool ok = false;
        // Loop to get a task
        do {
            ok = rpc_client_.SendRequest(stub, &Master_Stub::AssignTask,
                    &assign_request, &assign_response, 5, 1);
            if (!ok) {
                LOG(WARNING, "fail to get task from master: %s", master_endpoint_.c_str());
                sleep(FLAGS_suspend_time);
            }
        } while (!ok);

        if (assign_response.status() == kNoMore) {
            LOG(INFO, "master has no more task for minion, bye");
            break;
        } else if (assign_response.status() == kNoSuchJob) {
            LOG(WARNING, "current job may be finished: %s", FLAGS_jobid.c_str());
            break;
        } else if (assign_response.status() == kSuspend) {
            LOG(INFO, "minion will suspend for a while and retry");
            sleep(FLAGS_suspend_time);
            continue;
        } else if (assign_response.status() != kOk) {
            LOG(FATAL, "invalid response status: %s",
                    Status_Name(assign_response.status()).c_str());
            abort();
        }

        // Exec assigned task
        const TaskInfo& task = assign_response.task();
        SaveBreakpoint(task);
        {
            MutexLock lock(&mu_);
            task_id_ = task.task_id();
            attempt_id_ = task.attempt_id();
            state_ = kTaskRunning;
        }
        LOG(INFO, "try exec task: %s, phase %d, task %d, attempt %d",
                FLAGS_jobid.c_str(), FLAGS_node, task.task_id(), task.attempt_id());
        TaskState task_state = executor_->Exec(assign_response.job(), task);
        LOG(INFO, "exec done, task state: %s", TaskState_Name(task_state).c_str());
        {
            MutexLock lock(&mu_);
            state_ = task_state;
        }

        // ParseCounters
        std::map<std::string, int64_t> counters;
        const NodeConfig& cur_node = assign_response.job().nodes(task.node());
        if (task_state == kTaskCompleted && cur_node.check_counters()) {
            executor_->ParseCounters(counters);
        }

        // Finish current task
        FinishTaskRequest finish_request;
        FinishTaskResponse finish_response;
        finish_request.set_jobid(FLAGS_jobid);
        finish_request.set_node(FLAGS_node);
        finish_request.set_task_id(task.task_id());
        finish_request.set_attempt_id(task.attempt_id());
        //finish_request.set_task_state(task_state);
        finish_request.set_endpoint(endpoint_);
        for (std::map<std::string, int64_t>::iterator it = counters.begin();
                it != counters.end(); ++it) {
            TaskCounter* cur = finish_request.add_counters();
            cur->set_key(it->first);
            cur->set_value(it->second);
        }
        do {
            ok = rpc_client_.SendRequest(stub, &Master_Stub::FinishTask,
                    &finish_request, &finish_response, 5, 1);
            if (!ok || finish_response.status() == kSuspend) {
                LOG(WARNING, "task finishing needs some time, suspend");
                sleep(FLAGS_suspend_time);
                ok = false;
                continue;
            }
        } while (!ok);

        ClearBreakpoint();
        sleep(FLAGS_suspend_time);
    }
    delete stub;
}

void MinionImpl::Kill() {
    if (!GetMasterEndpoint()) {
        return;
    }
    CheckBreakpoint();
}

void MinionImpl::StopLoop() {
    running_ = false;
}

void MinionImpl::SaveBreakpoint(const TaskInfo& task) {
    FILE* breakpoint = ::fopen(FLAGS_breakpoint.c_str(), "w");
    if (breakpoint == NULL) {
        return;
    }
    ::fprintf(breakpoint, "%d %d\n", task.task_id(), task.attempt_id());
    ::fclose(breakpoint);
}

void MinionImpl::ClearBreakpoint() {
    if (::remove(FLAGS_breakpoint.c_str()) != 0) {
        LOG(WARNING, "fail to remove breakpoint file");
    }
}

void MinionImpl::CheckBreakpoint() {
    FILE* breakpoint = ::fopen(FLAGS_breakpoint.c_str(), "r");
    if (breakpoint == NULL) {
        return;
    }
    int task_id = 0;
    int attempt_id = 0;
    int ret = ::fscanf(breakpoint, "%d%d", &task_id, &attempt_id);
    ::fclose(breakpoint);
    if (ret != 2) {
        LOG(WARNING, "invalid breakpoint file");
        ClearBreakpoint();
        return;
    }
    Master_Stub* stub = NULL;
    if (!rpc_client_.GetStub(master_endpoint_, &stub) || stub == NULL) {
        /*
         * Force exit here since minion hasn't got any task
         * and restarted minion may report to master successfully
         */
        LOG(FATAL, "fail to get stub to master");
        abort();
    }
    FinishTaskRequest request;
    FinishTaskResponse response;
    LOG(INFO, "found unfinished task: id = %d, attempt = %d", task_id, attempt_id);
    request.set_jobid(FLAGS_jobid);
    request.set_task_id(task_id);
    request.set_attempt_id(attempt_id);
    request.set_task_state(kTaskKilled);
    request.set_endpoint(endpoint_);
    bool ok = rpc_client_.SendRequest(stub, &Master_Stub::FinishTask,
            &request, &response, 5, 1);
    if (!ok) {
        // Same as above
        LOG(FATAL, "fail to report unfinished task to master");
        abort();
    }
    delete stub;
}

bool MinionImpl::GetMasterEndpoint() {
    galaxy::ins::sdk::InsSDK ins_(FLAGS_nexus_addr);
    galaxy::ins::sdk::SDKError err;
    if (!ins_.Get(FLAGS_master_nexus_path, &master_endpoint_, &err)) {
        LOG(WARNING, "fail to get master endpoint from nexus, errno: %d", err);
        LOG(DEBUG, "nexus path of master: %s", FLAGS_master_nexus_path.c_str());
        return false;
    }
    return true;
}

}
}

