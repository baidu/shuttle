#include <gflags/gflags.h>
#include <boost/lexical_cast.hpp>
#include <sofa/pbrpc/pbrpc.h>
#include <string>
#include <signal.h>
#include "minion/container/minion_impl.h"
#include "util.h"

DECLARE_int32(minion_port);
DECLARE_string(jobid);
DECLARE_bool(kill);

static baidu::shuttle::MinionImpl* minion = NULL;

static void SignalIntHandler(int /*sig*/){
    if (minion != NULL) {
        minion->StopLoop();
    }
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_jobid.empty()) {
        LOG(baidu::WARNING, "use --jobid=[job id] to start minion");
        return 1;
    }
    minion = new baidu::shuttle::MinionImpl();
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    if (!rpc_server.RegisterService(static_cast<baidu::shuttle::Minion*>(minion))) {
        LOG(baidu::WARNING, "failed to register minion service");
        delete minion;
        minion = NULL;
        return -1;
    }

    int retry_count = 0;
    const std::string& hostname = baidu::common::util::GetLocalHostName();
    std::string endpoint = "0.0.0.0:" + boost::lexical_cast<std::string>(FLAGS_minion_port);
    std::string remote_ep = hostname + ":" + boost::lexical_cast<std::string>(FLAGS_minion_port);
    LOG(baidu::INFO, "hostname: %s", hostname.c_str());
    while (!rpc_server.Start(endpoint)) {
        LOG(baidu::WARNING, "failed to start server on %s", endpoint.c_str());
        if (++retry_count > 500) {
            LOG(baidu::WARNING, "cannot find free port");
            delete minion;
            minion = NULL;
            return -1;
        }
        const std::string& real_port = boost::lexical_cast<std::string>(FLAGS_minion_port + retry_count);
        endpoint = "0.0.0.0:" + real_port;
        remote_ep = hostname + ":" + real_port;
    }
    minion->SetEndpoint(remote_ep);
    if (FLAGS_kill) {
        minion->Kill();
        delete minion;
        minion = NULL;
        return 0;
    }
    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    LOG(baidu::INFO, "start minion...");
    minion->Run();
    LOG(baidu::INFO, "minion stopped.");
    delete minion;
    minion = NULL;
    return 0;
}

