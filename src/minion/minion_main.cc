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
#include "util.h"
#include "minion_impl.h"

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

DECLARE_int32(minion_port);
DECLARE_string(jobid);
DECLARE_int32(max_minions);

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
    std::string hostname = baidu::common::util::GetLocalHostName();
    std::string remote_ep = hostname + ":" + boost::lexical_cast<std::string>(FLAGS_minion_port);
    LOG(INFO, "hostname: %s", hostname.c_str());
    char* env_minion_port = getenv("GALAXY_PORT_MINION_PORT");
    if (env_minion_port != NULL) {
        std::string s_env_minion_port = env_minion_port;        
        endpoint = "0.0.0.0:" + s_env_minion_port;
        remote_ep = hostname + ":" + s_env_minion_port;
        if (!rpc_server.Start(endpoint)) {
            LOG(WARNING, "cannot find free port");
            _exit(-1);
        }
    } else {
        while (!rpc_server.Start(endpoint)) {
            LOG(WARNING, "failed to start server on %s", endpoint.c_str());
            if (++retry_count > FLAGS_max_minions) {
                LOG(WARNING, "cannot find free port");
                _exit(-1);
            }
            std::string real_port = boost::lexical_cast<std::string>(FLAGS_minion_port + retry_count);
            endpoint = "0.0.0.0:" + real_port;
            remote_ep = hostname + ":" + real_port;
        }
    }
    minion->SetEndpoint(remote_ep);
    minion->SetJobId(FLAGS_jobid);
    if (!minion->Run()) {
        LOG(WARNING, "fail to start minion.");
        _exit(-1);
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
