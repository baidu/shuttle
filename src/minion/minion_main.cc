#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <assert.h>
#include <algorithm>
#include <vector>
#include <boost/lexical_cast.hpp>
#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include "logging.h"
#include "minion_impl.h"

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

DECLARE_int32(minion_port);
DECLARE_string(jobid);

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
    s_quit = true;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_jobid.empty()) {
        LOG(WARNING, "use --jobid=[job id] to start minion");
        exit(-2);
    }
    baidu::shuttle::MinionImpl * minion = new baidu::shuttle::MinionImpl();
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    if (!rpc_server.RegisterService(static_cast<baidu::shuttle::Minion*>(minion))) {
        LOG(WARNING, "failed to register minion service");
        exit(-1);
    }

    int retry_count = 0;
    std::string endpoint = "0.0.0.0:" + boost::lexical_cast<std::string>(FLAGS_minion_port);
    while (!rpc_server.Start(endpoint)) {
        LOG(WARNING, "failed to start server on %s", endpoint.c_str());
        if (++retry_count > 500) {
            LOG(FATAL, "cannot find free port");
            exit(-1);
        }
        endpoint = "0.0.0.0:" + 
                     boost::lexical_cast<std::string>(FLAGS_minion_port + retry_count);
    }
    minion->SetEndpoint(endpoint);
    minion->SetJobId(FLAGS_jobid);
    if (!minion->Run()) {
        LOG(WARNING, "fail to start minion.");
        exit(-1);
    }
    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    LOG(INFO, "minion started.");
    while (!s_quit && !minion->IsStop()) {
        sleep(1);
    }
    LOG(INFO, "minion stopped.");
    return 0;
}
