#include <stdlib.h>
#include <signal.h>
#include <assert.h>
#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include "logging.h"
#include "master_impl.h"

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;

DECLARE_string(master_port);

namespace ins_common {
void SetLogLevel(int level);
}

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
    s_quit = true;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    ins_common::SetLogLevel(8);
    baidu::shuttle::MasterImpl * master = new baidu::shuttle::MasterImpl();
    master->Init();
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    if (!rpc_server.RegisterService(static_cast<baidu::shuttle::Master*>(master))) {
        LOG(FATAL, "failed to register master service");
        exit(-1);
    }

    std::string endpoint = "0.0.0.0:" + FLAGS_master_port;
    if (!rpc_server.Start(endpoint)) {
        LOG(FATAL, "failed to start server on %s", endpoint.c_str());
        exit(-2);
    }
    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);

    LOG(INFO, "master started.");
    while (!s_quit) {
        sleep(1);
    }
    return 0;
}

