#include "job_tracker.h"

#include <sstream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <assert.h>
#include <sys/time.h>

#include "common/filesystem.h"
#include "proto/serialize.pb.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

JobTracker::JobTracker(const JobDescriptor& job_descriptor) :
        job_(job_descriptor), state_(kPending), start_time_(0), finish_time_(0),
        scheduler_(job_descriptor) {
    job_id_ = GenerateJobId();
    grus_.resize(job_.nodes().size(), 0);
}

JobTracker::~JobTracker() {
    for (std::vector<Gru*>::iterator it = grus_.begin();
            it != grus_.end(); ++it) {
        delete *it;
    }
}

Status JobTracker::Start() {
    // XXX Maybe set to running until some assignment is required
    state_ = kRunning;
    Status ret_val = kOk;
    if (!scheduler_.Validate()) {
        LOG(WARNING, "job do not meet DAG limitation: %s", job_id_.c_str());
        return kInvalidArg;
    }
    const std::vector<int>& first = scheduler_.AvailableNodes();
    for (std::vector<int>::const_iterator it = first.begin();
            it != first.end(); ++it) {
        int node = *it;
        Gru*& cur = grus_[node];
        cur = Gru::GetAlphaGru(job_, job_id_, node, &scheduler_);
        cur->RegisterNearlyFinishCallback(
                boost::bind(&JobTracker::ScheduleNextPhase, this, node));
        cur->RegisterFinishedCallback(
                boost::bind(&JobTracker::FinishPhase, this, node, _1));
        Status ret = cur->Start();
        if (ret != kOk) {
            ret_val = ret;
            break;
        }
    }
    return ret_val;
}

Status JobTracker::Update(const std::string& priority, const std::vector<UpdateItem>& nodes) {
    int error_times = 0;
    if (!priority.empty()) {
        for (std::vector<Gru*>::iterator it = grus_.begin();
                it != grus_.end(); ++it) {
            Gru* cur = *it;
            if (cur == NULL) {
                continue;
            }
            JobState state = cur->GetState();
            if (state != kRunning && state != kPending) {
                continue;
            }
            if (cur->SetPriority(priority) != kOk) {
                ++error_times;
            }
        }
    }
    for (std::vector<UpdateItem>::const_iterator it = nodes.begin();
            it != nodes.end(); ++it) {
        if (static_cast<size_t>(it->node) > grus_.size() || grus_[it->node] == NULL) {
            continue;
        }
        Gru* cur = grus_[it->node];
        JobState state = cur->GetState();
        if (state != kRunning && state != kPending) {
            continue;
        }
        if (it->capacity != -1) {
            if (cur->SetCapacity(it->capacity) != kOk) {
                ++error_times;
            }
        }
    }
    return error_times ? kGalaxyError : kOk;
}

Status JobTracker::Kill() {
    for (std::vector<Gru*>::iterator it = grus_.begin();
            it != grus_.end(); ++it) {
        if (*it == NULL) {
            continue;
        }
        JobState state = (*it)->GetState();
        if (state == kPending || state == kRunning) {
            (*it)->Kill();
        }
    }
    FinishWholeJob(kKilled);
    return kOk;
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

Status JobTracker::GetStatistics(std::vector<TaskStatistics>& stats) {
    stats.clear();
    size_t size = grus_.size();
    stats.resize(size);
    for (size_t i = 0; i < size; ++i) {
        if (grus_[i] == NULL) {
            stats[i].set_total(0);
            stats[i].set_pending(0);
            stats[i].set_running(0);
            stats[i].set_failed(0);
            stats[i].set_killed(0);
            stats[i].set_completed(0);
        } else {
            stats[i].CopyFrom(grus_[i]->GetStatistics());
        }
    }
    return kOk;
}

Status JobTracker::GetTaskOverview(std::vector<TaskOverview>& tasks) {
    tasks.clear();
    for (size_t i = 0; i < grus_.size(); ++i) {
        if (grus_[i] == NULL) {
            continue;
        }
        std::vector<AllocateItem> cur_history;
        grus_[i]->GetHistory(cur_history);
        for (std::vector<AllocateItem>::iterator it = cur_history.begin();
                it != cur_history.end(); ++it) {
            TaskOverview task;
            TaskInfo* info = task.mutable_info();
            info->set_task_id(it->no);
            info->set_attempt_id(it->attempt);
            // TODO Fill input with proper information
            info->set_node(i);
            task.set_state(it->state);
            task.set_minion_addr(it->endpoint);
            task.set_progress(it->period == -1 ? 0 : 1);
            task.set_start_time(it->alloc_time);
            task.set_finish_time(it->period == -1 ? 0 : it->alloc_time + it->period);
            tasks.push_back(task);
        }
    }
    return kOk;
}

Status JobTracker::Load(const std::string& serialized) {
    JobTrackerCollection backup_jtc;
    backup_jtc.ParseFromString(serialized);
    job_id_ = backup_jtc.job_id();
    state_ = backup_jtc.state();
    start_time_ = backup_jtc.start_time();
    finish_time_ = backup_jtc.finish_time();
    ::google::protobuf::RepeatedPtrField<JobTrackerCollection_GruInfo>::const_iterator it;
    for (it = backup_jtc.grus().begin(); it != backup_jtc.grus().end(); ++it) {
        if (!it->has_serialized()) {
            grus_.push_back(NULL);
        } else {
            Gru* cur = NULL;
            int node = static_cast<int>(grus_.size());
            switch (it->type()) {
            case kAlphaGru: cur = Gru::GetAlphaGru(job_, job_id_, node, &scheduler_); break;
            case kBetaGru: cur = Gru::GetBetaGru(job_, job_id_, node, &scheduler_); break;
            case kOmegaGru: cur = Gru::GetOmegaGru(job_, job_id_, node, &scheduler_); break;
            }
            if (cur->Load(it->serialized()) == kInvalidArg) {
                state_ = kFailed;
                return kInvalidArg;
            }
            cur->RegisterNearlyFinishCallback(
                    boost::bind(&JobTracker::ScheduleNextPhase, this, node));
            cur->RegisterFinishedCallback(
                    boost::bind(&JobTracker::FinishPhase, this, node, _1));
            grus_.push_back(cur);
        }
    }
    return kOk;
}

std::string JobTracker::Dump() {
    JobTrackerCollection backup_jtc;
    // XXX May meet inconsistence due to late lock
    for (std::vector<Gru*>::iterator it = grus_.begin();
            it != grus_.end(); ++it) {
        JobTrackerCollection_GruInfo* cur = backup_jtc.add_grus();
        if (*it != NULL) {
            cur->set_type((*it)->GetType());
            cur->set_serialized((*it)->Dump());
        }
    }
    MutexLock lock(&meta_mu_);
    backup_jtc.set_job_id(job_id_);
    backup_jtc.set_state(state_);
    backup_jtc.set_start_time(start_time_);
    backup_jtc.set_finish_time(finish_time_);;
    std::string serialized;
    backup_jtc.SerializeToString(&serialized);
    return serialized;
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
            next = Gru::GetBetaGru(job_, job_id_, node, &scheduler_);
        } else {
            next = Gru::GetOmegaGru(job_, job_id_, node, &scheduler_);
        }
        next->RegisterNearlyFinishCallback(
                boost::bind(&JobTracker::ScheduleNextPhase, this, node));
        next->RegisterFinishedCallback(
                boost::bind(&JobTracker::FinishPhase, this, node, _1));
        if (next->Start() != kOk) {
            FinishWholeJob(kFailed);
            return;
        }
    }
}

void JobTracker::FinishPhase(int node, JobState state) {
    assert(state != kPending && state != kRunning);
    scheduler_.RemoveFinishedNode(node);
    if (state != kCompleted || scheduler_.UnfinishedNodes() == 0) {
        // XXX If retry is allowed, then re-loading it if it is failed
        FinishWholeJob(state);
    }
}

void JobTracker::FinishWholeJob(JobState state) {
    LOG(INFO, "finish a whole shuttle job: %s", job_id_.c_str());
    {
        MutexLock lock(&meta_mu_);
        state_ = state;
        finish_time_ = std::time(NULL);
    }
    if (finished_callback_ != 0) {
        finished_callback_();
    }
    // Notice gru to clean up the temporary dir
    for (std::vector<Gru*>::iterator it = grus_.begin();
            it != grus_.end(); ++it) {
        (*it)->CleanTempDir();
    }
}

}
}

