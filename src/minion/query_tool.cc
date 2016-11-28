#include "proto/minion.pb.h"

#include <boost/scoped_ptr.hpp>
#include <gflags/gflags.h>
#include <iostream>
#include "common/rpc_client.h"
#include "logging.h"

DEFINE_bool(a, false, "query result contains log message");

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (argc < 2) {
        std::cout << "./ping_tool minion_addr:port" << std::endl;
        return 1;
    }
    std::string endpoint(argv[1]);
    if (endpoint.find_first_of(":") == std::string::npos) {
        std::cout << "invalid endpoint format" << std::endl;
        return 1;
    }
    baidu::shuttle::RpcClient rpc;
    baidu::shuttle::Minion_Stub* stub = NULL;
    rpc.GetStub(endpoint, &stub);
    boost::scoped_ptr<baidu::shuttle::Minion_Stub> stub_guard(stub);
    baidu::shuttle::QueryRequest request;
    baidu::shuttle::QueryResponse response;
    if (FLAGS_a) {
        request.set_detail(true);
    }
    if (!rpc.SendRequest(stub, &baidu::shuttle::Minion_Stub::Query,
            &request, &response, 5, 1)) {
        LOG(baidu::WARNING, "rpc with minion is failed");
        return -1;
    }
    if (!response.has_task_id()) {
        std::cout << "minion is frozened, please try later" << std::endl;
        return -1;
    }
    static const char* task_state[] = {
        "pending", "running", "failed", "killed", "completed",
        "canceled", "move output failed", "", "", "", "unknown"
    };
    std::cout << "minion response:" << std::endl;
    std::cout << "job id: " << response.job_id() << std::endl;
    std::cout << "task id: " << response.task_id() << std::endl;
    std::cout << "attempt id: " << response.attempt_id() << std::endl;
    std::cout << "status: " << task_state[response.attempt_id()] << std::endl;
    if (FLAGS_a && response.has_log_msg()) {
        std::cout << "log file:" << std::endl;
        std::cout << response.log_msg() << std::endl;
    }
    return 0;
}

