#include <gflags/gflags.h>

DEFINE_string(master_port, "9917", "master listen port");
DEFINE_string(galaxy_address, "0.0.0.0:", "galaxy address for sdk");
DEFINE_int32(galaxy_deploy_step, 30, "galaxy option to determine the step of deploy");
DEFINE_string(minion_path, "ftp://", "minion ftp path for galaxy to fetch");
DEFINE_int32(input_block_size, 500 * 1024 * 1024, "max size of input that a single map can get");
DEFINE_int32(timeout_bound, 10, "timeout bound in seconds for a minion response");
DEFINE_int32(replica_num, 3, "max replicas of a single task");
DEFINE_int32(replica_begin, 100, "the last tasks that are suitable for end game strategy");
DEFINE_string(nexus_root_path, "/shuttle/", "root of nexus path, compatible with galaxy nexus system");
DEFINE_string(master_lock_path, "master_lock", "the key used for master to lock");
DEFINE_string(master_path, "master", "the key used for minion to find master");
DEFINE_string(nexus_server_list, "", "server list for nexus to store meta data");
DEFINE_int32(gc_interval, 600, "time interval for master recycle outdated job");
DEFINE_int32(retry_bound, 5, "retry times when a certain task failed before the job is considered failed");

