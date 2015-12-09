#include <gflags/gflags.h>

DEFINE_int32(minion_port, 7900, "minion listen port");
DEFINE_string(jobid, "", "the job id that minion works on");
DEFINE_string(nexus_addr, "", "nexus server list");
DEFINE_string(master_nexus_path, "/shuttle/master", "master address on nexus");
DEFINE_string(work_mode, "map", "there are 3 kinds: map, reduce, map-only");
DEFINE_bool(kill_task, false, "kill unfinished task");
DEFINE_int32(suspend_time, 120, "suspend time in seconds when receive suspend op");
DEFINE_int32(max_minions, 25, "max number of minions at one machine");
