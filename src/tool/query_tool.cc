#include "proto/minion.pb.h"

#define STRIP_FLAG_HELP 1
#include <gflags/gflags.h>
#include <boost/scoped_ptr.hpp>
#include <string>
#include <iostream>
#include "common/rpc_client.h"

DEFINE_bool(h, false, "show help info");
DECLARE_bool(help);

const std::string helper = "Beam - a tool to ping minion\n"
    "Usage: beam hostname:port\n\n"
    "Options:\n"
    "  -h, --help     show this help info\n";

int main(int argc, char** argv) {
    google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
    if (FLAGS_h || FLAGS_help) {
        std::cerr << helper;
        return 1;
    }
    if (argc != 2) {
        std::cerr << "beam: invalid parameter number, "
            << "use -h to check help info" << std::endl;
        return -1;
    }
    std::string endpoint(argv[1]);
    if (endpoint.find_first_of(":") == std::string::npos) {
        if (endpoint == "me") {
            std::cerr << "beam: okay, beam you up" << std::endl;
        } else {
            std::cerr << "beam: invalid endpoint format" << std::endl;
        }
        return -2;
    }
    baidu::shuttle::RpcClient rpc;
    baidu::shuttle::Minion_Stub* stub = NULL;
    rpc.GetStub(endpoint, &stub);
    boost::scoped_ptr<baidu::shuttle::Minion_Stub> stub_guard(stub);
    baidu::shuttle::QueryRequest request;
    baidu::shuttle::QueryResponse response;
    if (!rpc.SendRequest(stub, &baidu::shuttle::Minion_Stub::Query,
            &request, &response, 5, 1)) {
        std::cerr << "beam: lose connection with minion" << std::endl;
        return -3;
    }
    static const char* task_state[] = {
        "pending", "running", "failed", "killed", "completed",
        "canceled", "move output failed", "", "", "", "unknown"
    };
    std::cout << "minion response:" << std::endl;
    std::cout << "job id: " << response.job_id() << std::endl;
    std::cout << "node: " << response.node() << std::endl;
    std::cout << "task id: " << response.task_id() << std::endl;
    std::cout << "attempt id: " << response.attempt_id() << std::endl;
    std::cout << "status: " << task_state[response.task_state()] << std::endl;
    return 0;
}

