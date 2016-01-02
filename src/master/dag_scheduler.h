#ifndef _BAIDU_SHUTTLE_DAG_SCHEDULER_H_
#define _BAIDU_SHUTTLE_DAG_SCHEDULER_H_
#include <list>
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

    std::vector<int> AvailableNodes();
    std::vector<int> NextNodes(int node);
    bool RemoveFinishedNode(int node);
    int UnfinishedNodes() {
        MutexLock lock(&mu_);
        return left_;
    }

protected:
    void ConvertMap(const JobDescriptor& job);

protected:
    Mutex mu_;
    // Adjacency list to store the dependency map
    // Converted from the forward adjacency list in proto
    std::vector<DagNode> dependency_map_;
    // Dynamically adjust and record the in-degree of the map
    // When a node is finished the successors of his lose 1 indegree
    std::vector<int> indegree_;
    int left_;
};

}
}

#endif

