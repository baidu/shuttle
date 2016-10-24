#ifndef _BAIDU_SHUTTLE_GALAXY_HANDLER_H_
#define _BAIDU_SHUTTLE_GALAXY_HANDLER_H_
#include "cluster_handler.h"
#include "galaxy_sdk_appmaster.h"

namespace baidu {
namespace shuttle {

class GalaxyHandler : public ClusterHandler {
public:
    GalaxyHandler(JobDescriptor& job, const std::string& job_id, int node);
    virtual ~GalaxyHandler() {
        Kill();
        if (galaxy_ != NULL) {
            delete galaxy_;
            galaxy_ = NULL;
        }
    }

    virtual Status Start();
    virtual Status Kill();

    virtual Status SetCapacity(int capacity);

    virtual Status Load(const std::string& galaxy_jobid);
    virtual std::string Dump();

private:
    ::baidu::galaxy::sdk::JobDescription PrepareGalaxyJob(
            const std::string& name, const NodeConfig& node);

private:
    // For galaxy manangement
    static ::baidu::galaxy::sdk::AppMaster* galaxy_;
    std::string minion_id_;

    // Minion information
    std::string minion_name_;
    JobDescriptor& job_;
    const std::string job_id_;
    int node_;
    std::string node_str_;
};

}
}

#endif

