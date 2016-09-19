#include <gflags/gflags.h>
#include <cstdlib>
#include "minion/output/hopper.h"
#include "minion/common/log_name.h"
#include "common/file.h"
#include "common/fileformat.h"
#include "logging.h"

using namespace baidu::shuttle;
using baidu::WARNING;

DEFINE_string(pipe, "text", "user input/output type, text/seq is acceptable");
DEFINE_string(format, "text", "set output format, text/seq is acceptable");
DEFINE_string(partitioner, "keyhash", "partitioner type, keyhash/inthash is acceptable");
DEFINE_string(separator, "\t", "separator to split record");
DEFINE_int32(key_fields, 1, "number of key fields");
DEFINE_int32(partition_fields, 1, "number of partition fields");
DEFINE_int32(dest_num, 1, "number of next phase");

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
    Partitioner* pt = Partitioner::Get(p, FLAGS_separator,
            FLAGS_key_fields, FLAGS_partition_fields, FLAGS_dest_num);
    if (pt == NULL) {
        LOG(WARNING, "fail to get partitioner to parse key");
        exit(-1);
    }
    return pt;
}

static FormattedFile* GetStdinWrapper() {
    FileFormat format = kPlainText;
    if (FLAGS_pipe == "streaming") {
        format = kPlainText;
    } else if (FLAGS_pipe == "bistreaming") {
        format = kInfSeqFile;
    } else {
        LOG(WARNING, "unfamiliar pipe type: %s", format.c_str());
        exit(-1);
    }
    FormattedFile* fp = FormattedFile::Get(File::Get(kLocalFs, stdin), fileformat);
    if (fp == NULL) {
        LOG(WARNING, "fail to get formatted file and parse input");
        exit(-1);
    }
    return fp;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baidu::common::SetLogFile(GetLogName("./output.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./output.warning").c_str());

    Partitioner* partitioner = GetPartitioner();
    FormattedFile* fin = GetStdinWrapper();
    // TODO
    FormattedFile* fout = FormattedFile::Create();
    // TODO
    Hopper hopper();

    std::string key, value;
    while (fin->ReadRecord(key, value)) {
        HopperItem item;
        const std::string& raw_key = format == kPlainText ? value : key;
        item.dest = partitioner->Calc(raw_key, &item.key);
        // TODO
        item.record = FormattedFile::BuildRecord();
        if (hopper.Emit(&item) != kOk) {
            LOG(WARNING, "fail to emit data to output");
            exit(1);
        }
    }
    if (fp->Error() != kOk && fp->Error() != kNoMore) {
        LOG(WARNING, "read record stops due to %s", Status_Name(fp->Error()).c_str());
    }
    if (hopper.Flush() != kOk && hopper.Flush() != kNoMore) {
        LOG(WARNING, "fail to flush data to output");
    }
    delete fout;
    delete fin;
    delete partitioner;
    return -1;
}

