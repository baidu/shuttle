#ifndef _BAIDU_SHUTTLE_GRU_H_
#define _BAIDU_SHUTTLE_GRU_H_
#include <string>

#include <stdint.h>

#include "proto/shuttle.pb.h"
#include "galaxy.h"

namespace baidu {
namespace shuttle {

class Gru {

public:
    Gru(::baidu::galaxy::Galaxy* galaxy, JobDescriptor* job,
        const std::string& job_id, WorkMode mode);
    virtual ~Gru() { Kill(); }

    Status Start();
    Status Kill();
    Status Update(const std::string& priority, int capacity);

    static int additional_map_millicores;
    static int additional_reduce_millicores;
    static int64_t additional_map_memory;
    static int64_t additional_reduce_memory;

private:
    // For galaxy manangement
    ::baidu::galaxy::Galaxy* galaxy_;
    ::baidu::galaxy::JobDescription galaxy_job_;
    std::string minion_id_;

    // Minion information
    std::string minion_name_;
    JobDescriptor* job_;
    const std::string& job_id_;
    WorkMode mode_;
    std::string mode_str_;

};

}
}

#endif

