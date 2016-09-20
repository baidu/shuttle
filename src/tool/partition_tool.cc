#include "minion/output/partition.h"

#define STRIP_FLAG_HELP 1
#include <gflags/gflags.h>
#include <string>
#include <iostream>

DEFINE_int32(k, 1, "first n columns are considered as key");
DEFINE_int32(p, 1, "first n columns are used to partition");
DEFINE_string(s, " ", "separator in key");
DEFINE_string(partitioner, "keyhash", "partitioner type");
DEFINE_bool(h, false, "show help info");
DECLARE_bool(help);

const std::string helper = "Phaser - a tool to check results of partition\n"
    "Usage: phaser dest_num [options]\n\n"
    "Options:\n"
    "  -k <num>[default = 1]             treat first <num> columns as key\n"
    "  -p <num>[default = 1]             use first <num> columns to parition\n"
    "  -s <separator>[default = \" \"]     use <separator> to separate columns\n"
    "  -partitioner <partitioner>        use <partitioner> type to partition\n"
    "    [default = keyhash]             keyhash/inthash is valid\n"
    "  -h, --help                        show this help info\n\n"
    "Use stdin to get keys. A whole line is considered as a record\n";

int main(int argc, char** argv) {
    google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
    if (FLAGS_h || FLAGS_help) {
        std::cerr << helper;
        return 1;
    }
    int dest_num = 0;
    if (argc < 2) {
        std::cerr << "phaser: no destination number specified" << std::endl;
        return -1;
    }
    if ((dest_num = atoi(argv[1])) <= 0) {
        if (std::string(argv[1]) == "stun") {
            std::cerr << "phaser: set phasers to stun!" << std::endl;
        } else {
            std::cerr << "phaser: invalid destination number specified" << std::endl;
        }
        return -2;
    }
    baidu::shuttle::Partition pt;
    if (FLAGS_partitioner == "keyhash") {
        pt = baidu::shuttle::kKeyFieldBasedPartitioner;
    } else if (FLAGS_partitioner == "inthash") {
        pt = baidu::shuttle::kIntHashPartitioner;
    } else {
        std::cerr << "phaser: unfamiliar partitioner type: "
            << FLAGS_partitioner << std::endl;
        return -2;
    }
    baidu::shuttle::Partitioner* p = baidu::shuttle::Partitioner::Get(
            pt, FLAGS_s, FLAGS_k, FLAGS_p, dest_num);
    std::string record;
    std::string key;
    while (std::getline(std::cin, record)) {
        int dest = p->Calc(record, &key);
        std::cout << "destination: " << dest << ", key: " << key << std::endl;
    }
    delete p;
    return 0;
}

