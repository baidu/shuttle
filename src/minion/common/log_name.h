#ifndef _BAIDU_SHUTTLE_LOG_NAME_H_
#define _BAIDU_SHUTTLE_LOG_NAME_H_
#include <string>
#include "timer.h"

namespace baidu {
namespace shuttle {

static inline std::string GetLogName(const char* prefix) {
    int32_t tm = baidu::common::timer::now_time();
    char buf[4096];
    snprintf(buf, sizeof(buf), "%s.%d", prefix, tm);
    return buf;
}

}
}

#endif

