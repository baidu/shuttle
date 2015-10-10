#ifndef _BAIDU_SHUTTLE_COMMON_TOOLS_UTIL_H_
#define _BAIDU_SHUTTLE_COMMON_TOOLS_UTIL_H_
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
