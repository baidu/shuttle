#include "minion_impl.h"

namespace baidu {
namespace shuttle {

void MinionImpl::CancelJob(::google::protobuf::RpcController* controller,
                           const ::shuttle::CancelJobRequest* request,
                           ::shuttle::CancelJobResponse* response,
                           ::google::protobuf::Closure* done);

}
}

