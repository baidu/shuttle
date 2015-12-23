#include "job_tracker.h"

#include <sstream>
#include <boost/lexical_cast.hpp>
#include <sys/time.h>

namespace baidu {
namespace shuttle {

JobTracker::JobTracker(const JobDescriptor& job_descriptor) :
        job_(job_descriptor), state_(kPending), start_time_(0), finish_time_(0) {
    job_id_ = GenerateJobId();
}

JobTracker::~JobTracker() {
}

std::string JobTracker::GenerateJobId() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    std::stringstream ss;
    ss << "job_" << (t.tm_year + 1900) << (t.tm_mon + 1) << t.tm_mday
       << t.tm_hour << t.tm_min << t.tm_sec << "_"
       << boost::lexical_cast<std::string>(random());
    return ss.str();
}

void JobTracker::ScheduleNextPhase() {
}

void JobTracker::FinishPhase() {
}

}
}

