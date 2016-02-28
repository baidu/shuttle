#ifndef _BAIDU_SHUTTLE_DAG_SCHEDULER_H_
#define _BAIDU_SHUTTLE_DAG_SCHEDULER_H_
#include <vector>
#include "proto/shuttle.pb.h"
#include "mutex.h"

namespace baidu {
namespace shuttle {

struct DagNode {
    int node;
    std::vector<int> pre;
    std::vector<int> next;
};

class DagScheduler {
public:
    DagScheduler(const JobDescriptor& job);
    virtual ~DagScheduler();

    bool Validate();

    std::vector<int> AvailableNodes();
    std::vector<int> NextNodes(int node);
    bool RemoveFinishedNode(int node);
    int UnfinishedNodes() {
        MutexLock lock(&mu_);
        return left_;
    }

    std::vector<int> Sources();
    std::vector<int> Destinations();

    bool HasPredecessors(int node) {
        size_t n = static_cast<size_t>(node);
        if (n > dependency_map_.size()) {
            return false;
        }
        return dependency_map_[n].pre.empty();
    }

    bool HasSuccessors(int node) {
        size_t n = static_cast<size_t>(node);
        if (n > dependency_map_.size()) {
            return false;
        }
        return dependency_map_[n].next.empty();
    }

    std::vector<int> ZeroOutdegreeNodes();

protected:
    void ConvertMap(const JobDescriptor& job);

protected:
    Mutex mu_;
    // Adjacency list to store the dependency map
    // Converted from the forward adjacency list in proto
    // Read-only after initialized, lock-free
    std::vector<DagNode> dependency_map_;
    // Dynamically adjust and record the in-degree of the map
    // When a node is finished the successors of his lose 1 indegree
    std::vector<int> indegree_;
    int left_;
};

}
}

#endif

