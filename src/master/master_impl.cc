#include "master_impl.h"

namespace baidu {
namespace shuttle {

void MasterImpl::SubmitJob(::google::protobuf::RpcController* controller,
                           const ::shuttle::SubmitJobRequest* request,
                           ::shuttle::SubmitJobResponse* response,
                           ::google::protobuf::Closure* done);

void MasterImpl::UpdateJob(::google::protobuf::RpcController* controller,
                           const ::shuttle::UpdateJobRequest* request,
                           ::shuttle::UpdateJobResponse* response,
                           ::google::protobuf::Closure* done);

void MasterImpl::KillJob(::google::protobuf::RpcController* controller,
                         const ::shuttle::KillJobRequest* request,
                         ::shuttle::KillJobResponse* response,
                         ::google::protobuf::Closure* done);

void MasterImpl::ListJobs(::google::protobuf::RpcController* controller,
                          const ::shuttle::ListJobsRequest* request,
                          ::shuttle::ListJobsResponse* response,
                          ::google::protobuf::Closure* done);

void MasterImpl::ShowJob(::google::protobuf::RpcController* controller,
                         const ::shuttle::ShowJobRequest* request,
                         ::shuttle::ShowJobResponse* response,
                         ::google::protobuf::Closure* done);

void MasterImpl::AssignTask(::google::protobuf::RpcController* controller,
                            const ::shuttle::AssignTaskRequest* request,
                            ::shuttle::AssignTaskResponse* response,
                            ::google::protobuf::Closure* done);

void MasterImpl::FinishTask(::google::protobuf::RpcController* controller,
                            const ::shuttle::FinishTaskRequest* request,
                            ::shuttle::FinishTaskResponse* response,
                            ::google::protobuf::Closure* done);

}
}

