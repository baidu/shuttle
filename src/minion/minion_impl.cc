#include "minion_impl.h"

namespace baidu {
namespace shuttle {

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

}
}

