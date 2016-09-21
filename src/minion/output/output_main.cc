#include <gflags/gflags.h>
#include <cstdlib>
#include "minion/output/hopper.h"
#include "minion/output/partition.h"
#include "minion/common/log_name.h"
#include "minion/common/streaming.h"
#include "common/file.h"
#include "common/fileformat.h"
#include "logging.h"

using namespace baidu::shuttle;
using baidu::WARNING;

// Basic parameters
DEFINE_string(pipe, "streaming", "user input/output type, streaming/bistreaming is acceptable");
DEFINE_string(format, "text", "set output format, text/seq is acceptable");
// File system related
DEFINE_string(user, "", "set username to FS, empty means default");
DEFINE_string(password, "", "set password to FS, empty only when username is empty");
DEFINE_string(host, "", "set host of FS, overwritten by full address");
DEFINE_string(port, "", "set port of FS, overwritten by full address");
DEFINE_string(type, "", "set input FS type, overwritten by full address");
DEFINE_string(address, "", "set work dir, must be full address when absent host and port");
// Partition related
DEFINE_string(partitioner, "keyhash", "partitioner type, keyhash/inthash is acceptable");
DEFINE_string(separator, "\t", "separator to split record");
DEFINE_int32(key_fields, 1, "number of key fields");
DEFINE_int32(partition_fields, 1, "number of partition fields");
DEFINE_int32(dest_num, 1, "number of next phase");

static FileType type = kInfHdfs;

static void FillParam(File::Param& param) {
    if (!FLAGS_type.empty()) {
        if (FLAGS_type == "hdfs") {
            type = kInfHdfs;
        } else if (FLAGS_type == "local") {
            type = kLocalFs;
        }
    }
    std::string host, port, path;
    if (File::ParseFullAddress(FLAGS_address, &type, &host, &port, &path)) {
        FLAGS_host = host;
        FLAGS_port = port;
        FLAGS_address = path;
    }
    if (!FLAGS_host.empty()) {
        param["host"] = FLAGS_host;
    }
    if (!FLAGS_port.empty()) {
        param["port"] = FLAGS_port;
    }
    if (!FLAGS_user.empty()) {
        param["user"] = FLAGS_user;
    }
    if (!FLAGS_password.empty()) {
        param["password"] = FLAGS_password;
    }
}

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
    baidu::common::SetLogFile(GetLogName("./output.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./output.warning").c_str());

    File::Param param;
    FillParam(param);
    FileFormat format = kPlainText;
    if (FLAGS_format == "text") {
        format = kPlainText;
    } else if (FLAGS_format == "seq") {
        format = kInfSeqFile;
    } else {
        LOG(baidu::WARNING, "unfamiliar format: %s", FLAGS_format.c_str());
        return -1;
    }

    Partitioner* partitioner = GetPartitioner();
    FormattedFile* fp = GetStdinWrapper();

    Hopper hopper(FLAGS_address, param);
    bool text = FLAGS_pipe == "streaming";
    std::string key, value;
    while (fp->ReadRecord(key, value)) {
        HopperItem item;
        const std::string& raw_key = text ? value : key;
        item.dest = partitioner->Calc(raw_key, &item.key);
        item.record = FormattedFile::BuildRecord(format, item.key, value);
        if (hopper.Emit(&item) != kOk) {
            LOG(WARNING, "fail to emit data to output");
            return 1;
        }
    }
    if (fp->Error() != kOk && fp->Error() != kNoMore) {
        LOG(WARNING, "read record stops due to %s", Status_Name(fp->Error()).c_str());
    }
    Status status = hopper.Flush();
    if (status != kOk && status != kNoMore) {
        LOG(WARNING, "fail to flush data to output: %s", Status_Name(status).c_str());
    }
    delete fp;
    delete partitioner;
    return 0;
}

