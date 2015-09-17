#ifndef _BAIDU_SHUTTLE_MINION_H_
#define _BAIDU_SHUTTLE_MINION_H_
#include "proto/minion.pb.h"

namespace baidu {
namespace shuttle {

class MinionImpl : public Minion {
public:
    MinionImpl();
    virtual ~MinionImpl();

    void CancelJob(::google::protobuf::RpcController* controller,
                   const ::shuttle::CancelJobRequest* request,
                   ::shuttle::CancelJobResponse* response,
                   ::google::protobuf::Closure* done);

}

}
}

#endif

