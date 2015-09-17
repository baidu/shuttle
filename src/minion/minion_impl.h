#ifndef _BAIDU_SHUTTLE_MINION_H_
#define _BAIDU_SHUTTLE_MINION_H_
#include "proto/minion.pb.h"

namespace baidu {
namespace shuttle {

class MinionImpl : public Minion {
public:
    MinionImpl();
    virtual ~MinionImpl();

    void Query(::google::protobuf::RpcController* controller,
               const ::shuttle::QueryRequest* request,
               ::shuttle::QueryResponse* response,
               ::google::protobuf::Closure* done);
    void CancelTask(::google::protobuf::RpcController* controller,
                    const ::shuttle::CancelTaskRequest* request,
                    ::shuttle::CancelTaskResponse* response,
                    ::google::protobuf::Closure* done);

}

}
}

#endif

