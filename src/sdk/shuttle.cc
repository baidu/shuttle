#include "shuttle.h"
#include "proto/app_master.pb.h"
#include "common/rpc_client.h"

namespace baidu {
namespace shuttle {

class ShuttleImpl : public Shuttle {
public:
	ShuttleImpl(const std::string& master_addr);
	virtual ~ShuttleImpl();
    bool SubmitJob(const sdk::JobDescription& job_desc);
    bool UpdateJob(const std::string& job_id, 
                   const sdk::JobDescription& job_desc);
    bool KillJob(const std::string& job_id);
    bool ShowJob(const std::string& job_id, 
                 sdk::JobInstance* job,
                 std::vector<sdk::TaskInstance>* tasks);
    bool ListJobs(std::vector<sdk::JobInstance>* jobs);
private:
	std::string master_addr_;
	Master_Stub* master_stub_;
	RpcClient rpc_client_;
};

Shuttle* Shuttle::Connect(const std::string& master_addr) {
	return new ShuttleImpl(master_addr);
}

ShuttleImpl::ShuttleImpl(const std::string& master_addr) {
	master_addr_ = master_addr;
}

ShuttleImpl::~ShuttleImpl() {

}

bool ShuttleImpl::SubmitJob(const sdk::JobDescription& job_desc) {
	return true;
}

bool ShuttleImpl::UpdateJob(const std::string& job_id, 
                            const sdk::JobDescription& job_desc) {
	return true;
}

bool ShuttleImpl::KillJob(const std::string& job_id) {
	return true;
}

bool ShuttleImpl::ShowJob(const std::string& job_id, 
                          sdk::JobInstance* job,
                          std::vector<sdk::TaskInstance>* tasks) {
	return true;
}

bool ShuttleImpl::ListJobs(std::vector<sdk::JobInstance>* jobs) {
	return true;
}

} //namespace shuttle
} //namespace baidu

