#include "master_impl.h"

namespace baidu {
namespace shuttle {

MasterImpl::MasterImpl() {

}

MasterImpl::~MasterImpl() {

}

void MasterImpl::SubmitJob(::google::protobuf::RpcController* controller,
                           const ::baidu::shuttle::SubmitJobRequest* request,
                           ::baidu::shuttle::SubmitJobResponse* response,
                           ::google::protobuf::Closure* done) {}

void MasterImpl::UpdateJob(::google::protobuf::RpcController* controller,
                           const ::baidu::shuttle::UpdateJobRequest* request,
                           ::baidu::shuttle::UpdateJobResponse* response,
                           ::google::protobuf::Closure* done) {}

void MasterImpl::KillJob(::google::protobuf::RpcController* controller,
                         const ::baidu::shuttle::KillJobRequest* request,
                         ::baidu::shuttle::KillJobResponse* response,
                         ::google::protobuf::Closure* done) {}

void MasterImpl::ListJobs(::google::protobuf::RpcController* controller,
                          const ::baidu::shuttle::ListJobsRequest* request,
                          ::baidu::shuttle::ListJobsResponse* response,
                          ::google::protobuf::Closure* done) {}

void MasterImpl::ShowJob(::google::protobuf::RpcController* controller,
                         const ::baidu::shuttle::ShowJobRequest* request,
                         ::baidu::shuttle::ShowJobResponse* response,
                         ::google::protobuf::Closure* done) {}

void MasterImpl::AssignTask(::google::protobuf::RpcController* controller,
                            const ::baidu::shuttle::AssignTaskRequest* request,
                            ::baidu::shuttle::AssignTaskResponse* response,
                            ::google::protobuf::Closure* done) {
    static int dummy_test = 0;
    if (++dummy_test < 10) {
      response->set_status(kOk); 
    } else {
      response->set_status(kNoMore);
    }
    done->Run();
}

void MasterImpl::FinishTask(::google::protobuf::RpcController* controller,
                            const ::baidu::shuttle::FinishTaskRequest* request,
                            ::baidu::shuttle::FinishTaskResponse* response,
                            ::google::protobuf::Closure* done) {
    response->set_status(kOk);
    done->Run();
}

}
}

