#ifndef _BAIDU_SHUTTLE_MINION_IMPL_H_
#define _BAIDU_SHUTTLE_MINION_IMPL_H_
#include "proto/minion.pb.h"

#include "minion/container/executor.h"
#include "common/rpc_client.h"
#include "mutex.h"

namespace baidu {
namespace shuttle {

class MinionImpl : public Minion {
public:
    MinionImpl() : running_(true), task_id_(-1), attempt_id_(-1),
            state_(kTaskUnknown), executor_(NULL) { }
    ~MinionImpl() {
        if (executor_ != NULL) {
            delete executor_;
        }
    }

    void Query(::google::protobuf::RpcController* controller,
               const ::baidu::shuttle::QueryRequest* request,
               ::baidu::shuttle::QueryResponse* response,
               ::google::protobuf::Closure* done);
    void CancelTask(::google::protobuf::RpcController* controller,
                    const ::baidu::shuttle::CancelTaskRequest* request,
                    ::baidu::shuttle::CancelTaskResponse* response,
                    ::google::protobuf::Closure* done);
    void SetEndpoint(const std::string& endpoint);
    void Run();
    void Kill();
    void StopLoop();
private:
    void SaveBreakpoint(const TaskInfo& task);
    void ClearBreakpoint();
    void CheckBreakpoint();

    bool GetMasterEndpoint();
private:
    RpcClient rpc_client_;
    // Meta
    std::string endpoint_;
    std::string master_endpoint_;
    bool running_;

    // Current task
    Mutex mu_;
    int32_t task_id_;
    int32_t attempt_id_;
    TaskState state_;
    Executor* executor_;
};

}
}

#endif

