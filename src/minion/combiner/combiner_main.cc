#include <gflags/gflags.h>
#include <cstdlib>
#include "minion/combiner/combiner.h"
#include "minion/output/partition.h"
#include "minion/common/log_name.h"
#include "minion/common/streaming.h"
#include "common/file.h"
#include "common/scanner.h"
#include "logging.h"

using namespace baidu::shuttle;
using baidu::WARNING;

DEFINE_string(cmd, "cat", "user-defined combiner command");
DEFINE_string(pipe, "streaming", "user input/output type, streaming/bistreaming is acceptable");
DEFINE_string(partitioner, "keyhash", "partitioner type, keyhash/inthash is acceptable");
DEFINE_string(separator, "\t", "separator to split record");
DEFINE_int32(key_fields, 1, "number of key fields");

static Partitioner* GetPartitioner() {
    Partition p = kKeyFieldBasedPartitioner;
    if (FLAGS_partitioner == "keyhash") {
        p = kKeyFieldBasedPartitioner;
    } else if (FLAGS_partitioner == "inthash") {
        p = kIntHashPartitioner;
    } else {
        LOG(WARNING, "unfamiliar partitioner type: %s", FLAGS_partitioner.c_str());
        exit(-1);
    }
    // Partitioner is used to extract key, so the dest num is not important
    Partitioner* pt = Partitioner::Get(p, FLAGS_separator, FLAGS_key_fields, 1, 1);
    if (pt == NULL) {
        LOG(WARNING, "fail to get partitioner to parse key");
        exit(-1);
    }
    return pt;
}

static FormattedFile* GetStdinWrapper() {
    File* inner = File::Get(kLocalFs, stdin);
    if (inner == NULL) {
        LOG(WARNING, "fail to wrap stdin, die");
        exit(-1);
    }
    FormattedFile* fp = NULL;
    if (FLAGS_pipe == "streaming") {
        fp = new TextStream(inner);
    } else if (FLAGS_pipe == "bistreaming") {
        fp = new BinaryStream(inner);
    }
    if (fp == NULL) {
        LOG(WARNING, "fail to get formatted file and parse input of pipe %s",
                FLAGS_pipe.c_str());
        exit(-1);
    }
    return fp;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baidu::common::SetLogFile(GetLogName("./combiner.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./combiner.warning").c_str());

    Partitioner* partitioner = GetPartitioner();
    FormattedFile* fp = GetStdinWrapper();
    Combiner combiner(FLAGS_cmd);

    bool text = FLAGS_pipe == "streaming";
    std::string key, value;
    while (fp->ReadRecord(key, value)) {
        CombinerItem item;
        const std::string& raw_key = text ? value : key;
        partitioner->Calc(raw_key, &item.key);
        item.record = fp->BuildRecord(key, value);
        if (combiner.Emit(&item) != kOk) {
            LOG(WARNING, "fail to emit data to combiner");
            return 1;
        }
    }
    if (fp->Error() != kOk && fp->Error() != kNoMore) {
        LOG(WARNING, "read record stops due to %s", Status_Name(fp->Error()).c_str());
    }
    if (combiner.Flush() != kOk && combiner.Flush() != kNoMore) {
        LOG(WARNING, "fail to flush data to combiner");
    }

    delete fp;
    delete partitioner;
    return 0;
}

