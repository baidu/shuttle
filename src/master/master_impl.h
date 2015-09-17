#ifndef _BAIDU_SHUTTLE_MASTER_IMPL_H_
#define _BAIDU_SHUTTLE_MASTER_IMPL_H_
#include "proto/master.pb.h"

namespace baidu {
namespace shuttle {

class MasterImpl : public Master {
public:

    MasterImpl();
    virtual ~MasterImpl();

    void SubmitJob(::google::protobuf::RpcController* controller,
                   const ::shuttle::SubmitJobRequest* request,
                   ::shuttle::SubmitJobResponse* response,
                   ::google::protobuf::Closure* done);
    void UpdateJob(::google::protobuf::RpcController* controller,
                   const ::shuttle::UpdateJobRequest* request,
                   ::shuttle::UpdateJobResponse* response,
                   ::google::protobuf::Closure* done);
    void KillJob(::google::protobuf::RpcController* controller,
                 const ::shuttle::KillJobRequest* request,
                 ::shuttle::KillJobResponse* response,
                 ::google::protobuf::Closure* done);
    void ListJobs(::google::protobuf::RpcController* controller,
                  const ::shuttle::ListJobsRequest* request,
                  ::shuttle::ListJobsResponse* response,
                  ::google::protobuf::Closure* done);
    void ShowJob(::google::protobuf::RpcController* controller,
                 const ::shuttle::ShowJobRequest* request,
                 ::shuttle::ShowJobResponse* response,
                 ::google::protobuf::Closure* done);
    void AssignTask(::google::protobuf::RpcController* controller,
                    const ::shuttle::AssignTaskRequest* request,
                    ::shuttle::AssignTaskResponse* response,
                    ::google::protobuf::Closure* done);
    void FinishTask(::google::protobuf::RpcController* controller,
                    const ::shuttle::FinishTaskRequest* request,
                    ::shuttle::FinishTaskResponse* response,
                    ::google::protobuf::Closure* done);

};

}
}

#endif

