#include "connector.h"

#include "sdk/shuttle.h"
#include "client/config.h"
#include "ins_sdk.h"

namespace baidu {
namespace shuttle {

ShuttleConnector::ShuttleConnector(Configuration* config)
        : config_(config) {
    if (config == NULL) {
        return;
    }
    const std::string& master = GetMasterAddr();
    if (master.empty()) {
        return;
    }
    sdk_ = Shuttle::Connect(master);
}

int ShuttleConnector::Submit() {
    return -1;
}

int ShuttleConnector::Update() {
    return -1;
}

int ShuttleConnector::Kill() {
    return -1;
}

int ShuttleConnector::List() {
    return -1;
}

int ShuttleConnector::Status() {
    return -1;
}

int ShuttleConnector::Monitor() {
    return -1;
}

std::string ShuttleConnector::GetMasterAddr() {
    galaxy::ins::sdk::SDKError error;
    galaxy::ins::sdk::InsSDK nexus(config_->GetConf("nexus"));
    const std::string& master_path = config_->GetConf("nexus-root")
        + config_->GetConf("master");
    std::string master_addr;
    bool ok = nexus.Get(master_path, &master_addr, &error);
    return ok ? master_addr : "";
}

}
}

