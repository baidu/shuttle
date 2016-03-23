#include <gflags/gflags.h>

DEFINE_string(master_port, "9917", "master listen port");
DEFINE_string(galaxy_address, "0.0.0.0:", "galaxy address for sdk");
DEFINE_int32(galaxy_deploy_step, 30, "galaxy option to determine the step of deploy");
DEFINE_string(minion_path, "ftp://", "minion ftp path for galaxy to fetch");
DEFINE_int32(input_block_size, 500 * 1024 * 1024, "max size of input that a single map can get");
DEFINE_int32(first_sleeptime, 10, "timeout bound in seconds for a minion response");
DEFINE_int32(time_tolerance, 120, "longest time interval of the monitor sleep");
DEFINE_int32(replica_num, 3, "max replicas of a single task");
DEFINE_int32(replica_begin, 100, "the last tasks that are suitable for end game strategy");
DEFINE_int32(replica_begin_percent, 10, "the last percentage of tasks for end game strategy");
DEFINE_int32(left_percent, 120, "percentage of left minions when there's no more resource for minion");
DEFINE_int32(parallel_attempts, 4, "max running replica of a certain task");
DEFINE_string(nexus_root_path, "/shuttle/", "root of nexus path, compatible with galaxy nexus system");
DEFINE_string(master_lock_path, "master_lock", "the key used for master to lock");
DEFINE_string(master_path, "master", "the key used for minion to find master");
DEFINE_string(nexus_server_list, "", "server list for nexus to store meta data");
DEFINE_string(jobdata_header, "his_", "header of history item in nexus key data");
DEFINE_int32(gc_interval, 600, "time interval for master recycling outdated job");
DEFINE_int32(backup_interval, 60000, "millisecond time interval for master backup jobs information");
DEFINE_int32(retry_bound, 3, "retry times when a certain task failed before the job is considered failed");
DEFINE_bool(recovery, false, "whether fallen into recovery process at the beginning");
DEFINE_int32(master_rpc_thread_num, 12, "rpc thread num of master");
DEFINE_int32(max_counters_per_job, 10000, "max counters per job");
DEFINE_bool(enable_cpu_soft_limit, false, "enable cpu soft limit or not");
DEFINE_string(galaxy_node_label, "", "set deploying node label on Galaxy");
