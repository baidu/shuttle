#include "dag_scheduler.h"

namespace baidu {
namespace shuttle {

DagScheduler::DagScheduler(const JobDescriptor& job) : left_(0) {
    ConvertMap(job);
    left_ = static_cast<int>(dependency_map_.size());
}

DagScheduler::~DagScheduler() {
}

std::vector<int> DagScheduler::AvailableNodes() {
}

bool DagScheduler::RemoveFinishedNode(int node) {
}

void DagScheduler::ConvertMap(const JobDescriptor& job) {
}

}
}

