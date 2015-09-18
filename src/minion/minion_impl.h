#ifndef _BAIDU_SHUTTLE_MINION_H_
#define _BAIDU_SHUTTLE_MINION_H_
#include "thread_pool.h"
#include "mutex.h"
#include "common/rpc_client.h"
#include "proto/minion.pb.h"
#include "ins_sdk.h"

namespace baidu {
namespace shuttle {

class MinionImpl : public Minion {
public:
    MinionImpl();
    virtual ~MinionImpl();

    void Query(::google::protobuf::RpcController* controller,
               const ::baidu::shuttle::QueryRequest* request,
               ::baidu::shuttle::QueryResponse* response,
               ::google::protobuf::Closure* done);
    void CancelTask(::google::protobuf::RpcController* controller,
                    const ::baidu::shuttle::CancelTaskRequest* request,
                    ::baidu::shuttle::CancelTaskResponse* response,
                    ::google::protobuf::Closure* done);
    void SetEndpoint(const std::string& endpoint);
    void SetJobId(const std::string& jobid);
    bool Run();
    bool IsStop();
private:
    void Loop();
    TaskState DoWork(const TaskInfo& task);
    std::string endpoint_;
    ThreadPool pool_;
    std::string master_endpoint_;
    galaxy::ins::sdk::InsSDK ins_;
    bool stop_;
    Mutex mu_;
    RpcClient rpc_client_;
    std::string jobid_;
};

}
}

#endif

