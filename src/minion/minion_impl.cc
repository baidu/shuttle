#include "minion_impl.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <gflags/gflags.h>
#include "logging.h"
#include "proto/app_master.pb.h"

DECLARE_string(master_nexus_path);
DECLARE_string(nexus_addr);
DECLARE_string(work_mode);

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

MinionImpl::MinionImpl() : ins_(FLAGS_nexus_addr),
                           stop_(false) {
    if (FLAGS_work_mode == "map") {
        executor_ = Executor::GetExecutor(Executor::kMap);
    } else if (FLAGS_work_mode == "reduce") {
        executor_ = Executor::GetExecutor(Executor::kReduce);
    } else if (FLAGS_work_mode == "map-only") {
        executor_ = Executor::GetExecutor(Executor::kMapOnly);
    } else {
        LOG(FATAL, "unkown work mode: %s", FLAGS_work_mode.c_str());
        abort();
    }
}

MinionImpl::~MinionImpl() {
    delete executor_;
}

void MinionImpl::Query(::google::protobuf::RpcController* controller,
                       const ::baidu::shuttle::QueryRequest* request,
                       ::baidu::shuttle::QueryResponse* response,
                       ::google::protobuf::Closure* done) {
}

void MinionImpl::CancelTask(::google::protobuf::RpcController* controller,
                            const ::baidu::shuttle::CancelTaskRequest* request,
                            ::baidu::shuttle::CancelTaskResponse* response,
                            ::google::protobuf::Closure* done) {
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
    int task_count = 0;
    while (true) {
        LOG(INFO, "======== task:%d ========", ++task_count);
        ::baidu::shuttle::AssignTaskRequest request;
        ::baidu::shuttle::AssignTaskResponse response;
        request.set_endpoint(endpoint_);
        request.set_jobid(jobid_);
        bool ok = rpc_client_.SendRequest(stub, &Master_Stub::AssignTask, 
                                            &request, &response, 5, 1);
        if (!ok) {
            LOG(FATAL, "fail to fetch task from master[%s]", master_endpoint_.c_str());
            abort();            
        }
        if (response.status() == kNoMore) {
            LOG(INFO, "master has no more task for minion, so exit.");
            break;
        }
        const TaskInfo& task = response.task();

        TaskState task_state = executor_->Exec(task); //exec here~~

        ::baidu::shuttle::FinishTaskRequest fn_request;
        ::baidu::shuttle::FinishTaskResponse fn_response;
        fn_request.set_jobid(jobid_);
        fn_request.set_task_id(task.task_id());
        fn_request.set_attempt_id(task.attempt_id());
        fn_request.set_task_state(task_state);
        ok = rpc_client_.SendRequest(stub, &Master_Stub::FinishTask,
                                     &fn_request, &fn_response, 5, 1);
        if (!ok) {
            LOG(FATAL, "fail to send task state to master");
            abort();            
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

}
}
