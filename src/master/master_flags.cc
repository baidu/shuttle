#include <gflags/gflags.h>

// Used in galaxy_handler.cc
DEFINE_string(cluster_address, "0.0.0.0:", "entrance address of cluster");
DEFINE_string(galaxy_am_path, "", "galaxy app master path on nexus");
DEFINE_string(cluster_user, "shuttle", "username for access to cluster manager");
DEFINE_string(cluster_token, "", "token for access to cluster manager");
DEFINE_int32(galaxy_deploy_step, 30, "galaxy option to determine the step of deploy");
DEFINE_int32(max_minions_per_host, 5, "limit of minions on a single host");
DEFINE_string(galaxy_node_label, "", "node label for Galaxy deploymenet");
DEFINE_string(cluster_pool, "", "pools for minions to deploy");
DEFINE_bool(cpu_soft_limit, false, "switch for soft limit of cpu");
DEFINE_bool(memory_soft_limit, false, "switch for soft limit of memory");
DEFINE_string(minion_path, "ftp://", "minion ftp path for galaxy to fetch");
DEFINE_string(nexus_server_list, "", "server list for nexus to store meta data");
DEFINE_string(nexus_root_path, "/shuttle/", "root of nexus path, compatible with galaxy nexus system");
DEFINE_string(master_path, "master", "the key used for minion to find master");

// Used in resource_manager.cc
DEFINE_int32(input_block_size, 500 * 1024 * 1024, "max size of input that a single map can get");
DEFINE_int32(parallel_attempts, 5, "max running replica of a certain task");

// Used in gru.cc
DEFINE_int32(replica_begin, 100, "the last tasks that are suitable for end game strategy");
DEFINE_int32(replica_begin_percent, 10, "the last percentage of tasks for end game strategy");
DEFINE_int32(replica_num, 3, "max replicas of a single task");
DEFINE_int32(left_percent, 120, "percentage of left minions when there's no more resource for minion");
DEFINE_int32(first_sleeptime, 10, "timeout bound in seconds for a minion response");
DEFINE_int32(time_tolerance, 120, "longest time interval of the monitor sleep");
DEFINE_string(temporary_dir, "/_temporary/", "define the accepted temporary directory");

// Used in master_impl.cc
// nexus_server_list defined
DEFINE_bool(recovery, false, "whether fallen into recovery process at the beginning");
// nexus_root_path defined
DEFINE_string(master_lock_path, "master_lock", "the key used for master to lock");
// master_path defined
// master_port defined
DEFINE_int32(gc_interval, 600, "time interval for master recycling outdated job");
DEFINE_int32(backup_interval, 5000, "millisecond time interval for master backup jobs information");

// Used in master_main.cc
DEFINE_string(master_port, "9917", "master listen port");

