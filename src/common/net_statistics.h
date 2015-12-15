#ifndef _BAIDU_SHUTTLE_COMMON_NET_STATISTICS_H_
#define _BAIDU_SHUTTLE_COMMON_NET_STATISTICS_H_

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include "thread_pool.h"
#include "mutex.h"

namespace baidu {
namespace shuttle {

class NetStatistics {
public:
    NetStatistics(const std::string& if_name);

    int64_t GetSendSpeed(){
        return send_speed_;
    }

    int64_t GetRecvSpeed() {
        return recv_speed_;
    }

    bool Ok() {
        return ok_;
    }
private:
    void CheckStatistics(int64_t last_send_amount, int64_t last_recv_amount);
    bool GetCurNetAmount(int64_t* send_amount, int64_t* recv_amount);
    ThreadPool pool_;
    Mutex mu_;
    std::string if_name_;
    int64_t send_speed_;
    int64_t recv_speed_;
    bool ok_;
};

}
}

#endif
