#include "minion_impl.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <cstdlib>
#include <gflags/gflags.h>
#include "logging.h"
#include "proto/app_master.pb.h"

DECLARE_string(master_nexus_path);
DECLARE_string(nexus_addr);
DECLARE_string(work_mode);
DECLARE_string(jobid);
DECLARE_bool(kill_task);
DECLARE_int32(suspend_time);

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

const std::string sBreakpointFile = "./task_running";

MinionImpl::MinionImpl() : ins_(FLAGS_nexus_addr),
                           stop_(false) {
    if (FLAGS_work_mode == "map") {
        executor_ = Executor::GetExecutor(kMap);
        work_mode_ =  kMap;
    } else if (FLAGS_work_mode == "reduce") {
        executor_ = Executor::GetExecutor(kReduce);
        work_mode_ = kReduce;
    } else if (FLAGS_work_mode == "map-only") {
        executor_ = Executor::GetExecutor(kMapOnly);
        work_mode_ = kMapOnly;
    } else {
        LOG(FATAL, "unkown work mode: %s", FLAGS_work_mode.c_str());
        abort();
    }
    if (FLAGS_kill_task) {
       galaxy::ins::sdk::SDKError err;
       ins_.Get(FLAGS_master_nexus_path, &master_endpoint_, &err);
       if (err == galaxy::ins::sdk::kOK) {
           Master_Stub* stub;
           rpc_client_.GetStub(master_endpoint_, &stub);
           if (stub != NULL) {
               boost::scoped_ptr<Master_Stub> stub_guard(stub);
               CheckUnfinishedTask(stub);
               _exit(0);
           }
       } else {
           LOG(WARNING, "fail to connect nexus");
       }
    }
    cur_task_id_ = -1;
    cur_attempt_id_ = -1;
    cur_task_state_ = kTaskUnknown;
}

MinionImpl::~MinionImpl() {
    delete executor_;
}

void MinionImpl::Query(::google::protobuf::RpcController* controller,
                       const ::baidu::shuttle::QueryRequest* request,
                       ::baidu::shuttle::QueryResponse* response,
                       ::google::protobuf::Closure* done) {
    (void)controller;
    (void)request;
    MutexLock locker(&mu_);
    response->set_job_id(jobid_);
    response->set_task_id(cur_task_id_);
    response->set_attempt_id(cur_attempt_id_);
    response->set_task_state(cur_task_state_);
    done->Run();
}

void MinionImpl::CancelTask(::google::protobuf::RpcController* controller,
                            const ::baidu::shuttle::CancelTaskRequest* request,
                            ::baidu::shuttle::CancelTaskResponse* response,
                            ::google::protobuf::Closure* done) {
    (void)controller;
    int32_t task_id = request->task_id();
    {
        MutexLock locker(&mu_);
        if (task_id != cur_task_id_) {
            response->set_status(kNoSuchTask);
        } else {
            executor_->Stop(task_id);
            response->set_status(kOk);
        }
    }
    done->Run();
}

void MinionImpl::SetEndpoint(const std::string& endpoint) {
    LOG(INFO, "minon bind endpoint on : %s", endpoint.c_str());
    endpoint_ = endpoint;
}

void MinionImpl::SetJobId(const std::string& jobid) {
    LOG(INFO, "minion will work on job: %s", jobid.c_str());
    jobid_ = jobid;
}

void MinionImpl::Loop() {
    Master_Stub* stub;
    rpc_client_.GetStub(master_endpoint_, &stub);
    if (stub == NULL) {
        LOG(FATAL, "fail to get master stub");
    }
    boost::scoped_ptr<Master_Stub> stub_guard(stub);
    int task_count = 0;
    CheckUnfinishedTask(stub);
    while (!stop_) {
        LOG(INFO, "======== task:%d ========", ++task_count);
        ::baidu::shuttle::AssignTaskRequest request;
        ::baidu::shuttle::AssignTaskResponse response;
        request.set_endpoint(endpoint_);
        request.set_jobid(jobid_);
        request.set_work_mode(work_mode_);
        LOG(INFO, "endpoint: %s", endpoint_.c_str());
        LOG(INFO, "jobid_: %s", jobid_.c_str());
        bool ok = rpc_client_.SendRequest(stub, &Master_Stub::AssignTask, 
                                          &request, &response, 5, 1);
        if (!ok) {
            LOG(FATAL, "fail to fetch task from master[%s]", master_endpoint_.c_str());
            abort();
        }
        if (response.status() == kNoMore) {
            LOG(INFO, "master has no more task for minion, so exit.");
            break;
        } else if (response.status() == kNoSuchJob) {
            LOG(INFO, "the job may be finished.");
            break;
        } else if (response.status() == kSuspend) {
            LOG(INFO, "minion will suspend for a while");
            sleep(FLAGS_suspend_time);
            continue;
        } else if (response.status() != kOk) {
            LOG(FATAL, "invalid responst status: %s",
                Status_Name(response.status()).c_str());
        }
        const TaskInfo& task = response.task();
        SaveBreakpoint(task);
        executor_->SetEnv(jobid_, task);
        {
            MutexLock locker(&mu_);
            cur_task_id_ = task.task_id();
            cur_attempt_id_ = task.attempt_id();
            cur_task_state_ = kTaskRunning;
        }
        LOG(INFO, "try exec task: %s, %d, %d", jobid_.c_str(), cur_task_id_, cur_attempt_id_);
        TaskState task_state = executor_->Exec(task); //exec here~~
        {
            MutexLock locker(&mu_);
            cur_task_state_ = task_state;
        }
        LOG(INFO, "exec done, task state: %s", TaskState_Name(task_state).c_str());
        ::baidu::shuttle::FinishTaskRequest fn_request;
        ::baidu::shuttle::FinishTaskResponse fn_response;
        fn_request.set_jobid(jobid_);
        fn_request.set_task_id(task.task_id());
        fn_request.set_attempt_id(task.attempt_id());
        fn_request.set_task_state(task_state);
        fn_request.set_endpoint(endpoint_);
        fn_request.set_work_mode(work_mode_);
        ok = rpc_client_.SendRequest(stub, &Master_Stub::FinishTask,
                                     &fn_request, &fn_response, 5, 1);
        if (!ok) {
            LOG(FATAL, "fail to send task state to master");
            abort();            
        }
        ClearBreakpoint();
        if (task_state == kTaskFailed) {
            LOG(WARNING, "task state: %s", TaskState_Name(task_state).c_str());
            executor_->ReportErrors(task, (work_mode_ != kReduce));
        }
    }

    {
        MutexLock locker(&mu_);
        stop_ = true;
    }
}

bool MinionImpl::IsStop() {
    return stop_;
}

bool MinionImpl::Run() {
    galaxy::ins::sdk::SDKError err;
    ins_.Get(FLAGS_master_nexus_path, &master_endpoint_, &err);
    if (err != galaxy::ins::sdk::kOK) {
        LOG(WARNING, "failed to fetch master endpoint from nexus, errno: %d", err);
        LOG(WARNING, "master_endpoint (%s) -> %s", FLAGS_master_nexus_path.c_str(),
            master_endpoint_.c_str());
        return false;
    }
    pool_.AddTask(boost::bind(&MinionImpl::Loop, this));
    return true;
}

void MinionImpl::CheckUnfinishedTask(Master_Stub* master_stub) {
    FILE* breakpoint = fopen(sBreakpointFile.c_str(), "r");
    int task_id;
    int attempt_id;
    if (breakpoint) {
        int n_ret = fscanf(breakpoint, "%d%d", &task_id, &attempt_id);
        if (n_ret != 2) {
            LOG(WARNING, "invalid breakpoint file");
            return;
        }
        fclose(breakpoint);
        ::baidu::shuttle::FinishTaskRequest fn_request;
        ::baidu::shuttle::FinishTaskResponse fn_response;
        LOG(WARNING, "found unfinished task: task_id: %d, attempt_id: %d", task_id, attempt_id);
        fn_request.set_jobid(FLAGS_jobid);
        fn_request.set_task_id(task_id);
        fn_request.set_attempt_id(attempt_id);
        fn_request.set_task_state(kTaskKilled);
        fn_request.set_endpoint(endpoint_);
        fn_request.set_work_mode(work_mode_);
        bool ok = rpc_client_.SendRequest(master_stub, &Master_Stub::FinishTask,
                                     &fn_request, &fn_response, 5, 1);
        if (!ok) {
            LOG(FATAL, "fail to report unfinished task to master");
            abort();
        }
    }
}

void MinionImpl::SaveBreakpoint(const TaskInfo& task) {
    FILE* breakpoint = fopen(sBreakpointFile.c_str(), "w");
    if (breakpoint) {
        fprintf(breakpoint, "%d %d\n", task.task_id(), task.attempt_id());
        fclose(breakpoint);
    }
}

void MinionImpl::ClearBreakpoint() {
    if (remove(sBreakpointFile.c_str()) != 0 ) {
        LOG(WARNING, "failed to remove breakponit file");
    }
}

}
}
