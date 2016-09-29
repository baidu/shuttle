#include <gflags/gflags.h>

DEFINE_int32(minion_port, 7900, "minion listen port");
DEFINE_string(jobid, "", "the job id that minion works on");
DEFINE_bool(kill, false, "kill unfinished task");

DEFINE_string(breakpoint, "./task_running", "path of breakpoint file");
DEFINE_string(nexus_addr, "", "nexus server list");
DEFINE_string(master_nexus_path, "/shuttle/master", "master address on nexus");
DEFINE_int32(node, 0, "current phase of the minion");
DEFINE_int32(suspend_time, 5, "suspend time in seconds when receive suspend op");

DEFINE_int32(max_counter, 10000, "max size of counters");
DEFINE_string(temporary_dir, "_temporary/", "temporary dir that stores internal files");

