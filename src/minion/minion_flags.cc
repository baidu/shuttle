#include <gflags/gflags.h>

DEFINE_int32(minion_port, 7900, "minion listen port");
DEFINE_string(jobid, "", "the job id that minion works on");
DEFINE_string(nexus_addr, "", "nexus server list");
DEFINE_string(master_nexus_path, "/shuttle/master", "master address on nexus");
