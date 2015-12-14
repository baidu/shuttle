#include "net_statistics.h"
#include <stdio.h>
#include <boost/bind.hpp>
#include <boost/function.hpp>

namespace baidu {
namespace shuttle {

const static int32_t sNetStatInterval = 5000; //5 seconds

NetStatistics::NetStatistics(const std::string& if_name) : if_name_(if_name) {
    send_speed_ = 0L;
    recv_speed_ = 0L;
    int64_t send_amount = -1;
    int64_t recv_amount = -1;
    bool ok = GetCurNetAmount(&send_amount, &recv_amount);
    if (!ok) {
        fprintf(stderr, "fail to get network statistics from /sys/class/net/%s\n", if_name.c_str());
        return;
    }
    pool_.DelayTask(sNetStatInterval, boost::bind(&NetStatistics::CheckStatistics, this, send_amount, recv_amount));
}

bool NetStatistics::GetCurNetAmount(int64_t* send_amount, int64_t* recv_amount) {
    std::string recv_bytes_file = std::string("/sys/class/net/") + if_name_ + "/statistics/rx_bytes";
    std::string send_bytes_file = std::string("/sys/class/net/") + if_name_ + "/statistics/tx_bytes";
    FILE* file = fopen(recv_bytes_file.c_str(), "r");
    if (file) {
        fscanf(file, "%lld", recv_amount);
        fclose(file);
    } else {
        return false;
    }
    file = fopen(send_bytes_file.c_str(), "r");
    if (file) {
        fscanf(file, "%lld", send_amount);
        fclose(file);
    } else {
        return false;
    }
    return true;
}

void NetStatistics::CheckStatistics(int64_t last_send_amount, int64_t last_recv_amount) {
    int64_t send_amount = -1;
    int64_t recv_amount = -1;
    GetCurNetAmount(&send_amount, &recv_amount);
    {
        MutexLock lock(&mu_);
        send_speed_ = (send_amount - last_send_amount) / (sNetStatInterval / 1000);
        recv_speed_ = (recv_amount - last_recv_amount) / (sNetStatInterval / 1000);
    }
    pool_.DelayTask(sNetStatInterval, boost::bind(&NetStatistics::CheckStatistics, this, send_amount, recv_amount));
}

}
}

