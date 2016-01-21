#include "job_tracker.h"

#include <sstream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <sys/time.h>

#include "common/filesystem.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

JobTracker::JobTracker(const JobDescriptor& job_descriptor) :
        job_(job_descriptor), state_(kPending), start_time_(0), finish_time_(0),
        scheduler_(job_descriptor), fs_(NULL) {
    job_id_ = GenerateJobId();
    grus_.resize(job_.nodes().size(), 0);
}

JobTracker::~JobTracker() {
}

Status JobTracker::Start() {
    // TODO Maybe set to running until some assignment is required
    state_ = kRunning;
    Status ret_val = kOk;
    const std::vector<int>& first = scheduler_.AvailableNodes();
    for (std::vector<int>::const_iterator it = first.begin();
            it != first.end(); ++it) {
        int node = *it;
        Gru*& cur = grus_[node];
        // TODO Create temp dir
        cur = Gru::GetAlphaGru(job_, job_id_, node);
        cur->RegisterNearlyFinishCallback(
                boost::bind(&JobTracker::ScheduleNextPhase, this, node));
        cur->RegisterFinishedCallback(
                boost::bind(&JobTracker::FinishPhase, this, node));
        Status ret = cur->Start();
        if (ret != kOk) {
            ret_val = ret;
            break;
        }
    }
    return ret_val;
}

Status JobTracker::Update(const std::vector<UpdateItem>& nodes) {
    int error_times = 0;
    for (std::vector<UpdateItem>::const_iterator it = nodes.begin();
            it != nodes.end(); ++it) {
        if (static_cast<size_t>(it->node) > grus_.size() || grus_[it->node] == NULL) {
            continue;
        }
        Gru* cur = grus_[it->node];
        JobState state = cur->GetState();
        if (state != kRunning || state != kPending) {
            continue;
        }
        if (it->capacity != -1) {
            if (cur->SetCapacity(it->capacity) != kOk) {
                ++error_times;
            }
        }
        if (!it->priority.empty()) {
            if (cur->SetPriority(it->priority)) {
                ++error_times;
            }
        }
    }
    return error_times ? kGalaxyError : kOk;
}

Status JobTracker::Kill() {
    // TODO Implement here
    return kUnKnown;
}

ResourceItem* JobTracker::Assign(int node, const std::string& endpoint, Status* status) {
    size_t n = static_cast<size_t>(node);
    if (n > grus_.size() || grus_[n] == NULL) {
        LOG(WARNING, "inexist node, assign terminated: %s", job_id_.c_str());
        return NULL;
    }
    return grus_[n]->Assign(endpoint, status);
}

Status JobTracker::Finish(int node, int no, int attempt, TaskState state) {
    size_t n = static_cast<size_t>(node);
    if (n > grus_.size() || grus_[n] == NULL) {
        LOG(WARNING, "inexist node, finish terminated: %s", job_id_.c_str());
        return kNoSuchJob;
    }
    return grus_[n]->Finish(no, attempt, state);
}

std::string JobTracker::GenerateJobId() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    std::stringstream ss;
    ss << "job_" << (t.tm_year + 1900) << (t.tm_mon + 1) << t.tm_mday << "_"
       << t.tm_hour << t.tm_min << t.tm_sec << "_"
       << boost::lexical_cast<std::string>(random());
    return ss.str();
}

void JobTracker::ScheduleNextPhase(int node) {
    const std::vector<int>& next = scheduler_.NextNodes(node);
    for (std::vector<int>::const_iterator it = next.begin();
            it != next.end(); ++it) {
        int node = *it;
        Gru*& next = grus_[node];
        if (next != NULL) {
            continue;
        }
        if (scheduler_.HasSuccessors(node)) {
            next = Gru::GetBetaGru(job_, job_id_, node);
        } else {
            next = Gru::GetOmegaGru(job_, job_id_, node);
        }
        next->RegisterNearlyFinishCallback(
                boost::bind(&JobTracker::ScheduleNextPhase, this, node));
        next->RegisterFinishedCallback(
                boost::bind(&JobTracker::FinishPhase, this, node));
        next->Start();
    }
}

void JobTracker::FinishPhase(int node) {
    scheduler_.RemoveFinishedNode(node);
    if (scheduler_.UnfinishedNodes() == 0) {
        FinishWholeJob();
    }
}

void JobTracker::FinishWholeJob() {
    LOG(INFO, "finish a whole shuttle job: %s", job_id_.c_str());
    // TODO Clean temp dir
}

}
}

