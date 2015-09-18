#ifndef _BAIDU_SHUTTLE_MASTER_IMPL_H_
#define _BAIDU_SHUTTLE_MASTER_IMPL_H_
#include <string>
#include <map>

#include "galaxy.h"
#include "mutex.h"
#include "proto/master.pb.h"
#include "job_tracker.h"

namespace baidu {
namespace shuttle {

class MasterImpl : public Master {
public:

    MasterImpl();
    virtual ~MasterImpl();

    void SubmitJob(::google::protobuf::RpcController* controller,
                   const ::baidu::shuttle::SubmitJobRequest* request,
                   ::baidu::shuttle::SubmitJobResponse* response,
                   ::google::protobuf::Closure* done);
    void UpdateJob(::google::protobuf::RpcController* controller,
                   const ::baidu::shuttle::UpdateJobRequest* request,
                   ::baidu::shuttle::UpdateJobResponse* response,
                   ::google::protobuf::Closure* done);
    void KillJob(::google::protobuf::RpcController* controller,
                 const ::baidu::shuttle::KillJobRequest* request,
                 ::baidu::shuttle::KillJobResponse* response,
                 ::google::protobuf::Closure* done);
    void ListJobs(::google::protobuf::RpcController* controller,
                  const ::baidu::shuttle::ListJobsRequest* request,
                  ::baidu::shuttle::ListJobsResponse* response,
                  ::google::protobuf::Closure* done);
    void ShowJob(::google::protobuf::RpcController* controller,
                 const ::baidu::shuttle::ShowJobRequest* request,
                 ::baidu::shuttle::ShowJobResponse* response,
                 ::google::protobuf::Closure* done);
    void AssignTask(::google::protobuf::RpcController* controller,
                    const ::baidu::shuttle::AssignTaskRequest* request,
                    ::baidu::shuttle::AssignTaskResponse* response,
                    ::google::protobuf::Closure* done);
    void FinishTask(::google::protobuf::RpcController* controller,
                    const ::baidu::shuttle::FinishTaskRequest* request,
                    ::baidu::shuttle::FinishTaskResponse* response,
                    ::google::protobuf::Closure* done);

private:
    ::baidu::galaxy::Galaxy* galaxy_sdk_;
    Mutex tracker_mu_;
    std::map<std::string, JobTracker*> job_trackers_;
};

}
}

#endif

