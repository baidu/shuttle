#include "dag_scheduler.h"
#include <queue>

namespace baidu {
namespace shuttle {

DagScheduler::DagScheduler(const JobDescriptor& job) : left_(0) {
    ConvertMap(job);
    left_ = static_cast<int>(dependency_map_.size());
}

DagScheduler::~DagScheduler() {
}

bool DagScheduler::Validate() {
    std::vector<int> indegree(indegree_);
    std::queue<int> working;
    do {
        if (!working.empty()) {
            int front = working.front();
            working.pop();
            const std::vector<int>& nexts = dependency_map_[front].next;
            for (std::vector<int>::const_iterator it = nexts.begin();
                    it != nexts.end(); ++it) {
                --indegree[*it];
            }
        }
        while (std::find(indegree.begin(), indegree.end(), 0) != indegree.end()) {
            std::vector<int>::iterator cur = std::find(indegree.begin(), indegree.end(), 0);
            working.push(cur - indegree.begin());
            *cur = -1;
        }
    } while (!working.empty());
    bool all_clear = true;
    for (std::vector<int>::iterator it = indegree.begin();
            it != indegree.end(); ++it) {
        if (*it != -1) {
            all_clear = false;
            break;
        }
    }
    return all_clear;
}

std::vector<int> DagScheduler::AvailableNodes() {
    std::vector<int> available;
    mu_.Lock();
    for (size_t i = 0; i < indegree_.size(); ++i) {
        if (indegree_[i] == 0) {
            available.push_back(i);
        }
    }
    mu_.Unlock();
    return available;
}

std::vector<int> DagScheduler::NextNodes(int node) {
    if (node == -1) {
        return Sources();
    }
    size_t n = static_cast<int>(node);
    if (n > dependency_map_.size()) {
        return std::vector<int>();
    }
    return dependency_map_[n].next;
}

bool DagScheduler::RemoveFinishedNode(int node) {
    size_t cur = static_cast<size_t>(node);
    // size of indegree is inchangable so there's no need to lock
    if (cur > indegree_.size()) {
        return false;
    }
    MutexLock lock(&mu_);
    indegree_[cur] = -1;
    for (std::vector<int>::iterator it = dependency_map_[cur].next.begin();
            it != dependency_map_[cur].next.end(); ++it) {
        --indegree_[*it];
    }
    --left_;
    return true;
}

std::vector<int> DagScheduler::Sources() {
    std::vector<int> src;
    for (std::vector<DagNode>::iterator it = dependency_map_.begin();
            it != dependency_map_.end(); ++it) {
        if (it->pre.empty()) {
            src.push_back(it->node);
        }
    }
    return src;
}

std::vector<int> DagScheduler::Destinations() {
    std::vector<int> dest;
    for (std::vector<DagNode>::iterator it = dependency_map_.begin();
            it != dependency_map_.end(); ++it) {
        if (it->next.empty()) {
            dest.push_back(it->node);
        }
    }
    return dest;
}

void DagScheduler::ConvertMap(const JobDescriptor& job) {
    size_t nodes = job.map().size();

    dependency_map_.clear();
    dependency_map_.resize(nodes);
    for (size_t i = 0; i < nodes; ++i) {
        DagNode& cur = dependency_map_[i];
        cur.node = static_cast<int>(i);
        size_t next_num = job.map(i).next().size();
        for (size_t j = 0; j < next_num; ++j) {
            int cur_neighbor = job.map(i).next(j);
            cur.next.push_back(cur_neighbor);
            dependency_map_[cur_neighbor].pre.push_back(i);
        }
    }
    
    indegree_.clear();
    for (size_t i = 0; i < nodes; ++i) {
        indegree_.push_back(dependency_map_[i].pre.size());
    }
}

}
}

