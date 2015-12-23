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
    int pre;
    int next;
};

struct AdjancencyListNode {
    int node;
    std::list< DagNode* > depends;
};

class DagScheduler {
public:
    DagScheduler(const JobDescriptor& job);
    virtual ~DagScheduler();

    std::vector<int> AvailableNodes();
    bool RemoveFinishedNode(int node);
    int UnfinishedNodes() {
        MutexLock lock(&mu_);
        return left_;
    }

private:
    void ConvertMap(const JobDescriptor& job);

private:
    Mutex mu_;
    // Backward adjacency list to store the dependency map
    // Converted from the forward adjacency list in proto
    std::list<AdjancencyListNode> dependency_map_;
    int left_;

};

}
}

#endif

