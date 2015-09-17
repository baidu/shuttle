#include "minion_impl.h"

namespace baidu {
namespace shuttle {

void MinionImpl::Query(::google::protobuf::RpcController* controller,
                       const ::shuttle::QueryRequest* request,
                       ::shuttle::QueryResponse* response,
                       ::google::protobuf::Closure* done);

void MinionImpl::CancelTask(::google::protobuf::RpcController* controller,
                            const ::shuttle::CancelTaskRequest* request,
                            ::shuttle::CancelTaskResponse* response,
                            ::google::protobuf::Closure* done);

}
}

