#include <gflags/gflags.h>
#include <boost/scoped_ptr.hpp>
#include "logging.h"
#include "minion/input/inlet.h"
#include "minion/common/log_name.h"
#include "common/file.h"

using namespace baidu::shuttle;

// Basic parameters
DEFINE_string(pipe, "streaming", "set pipe type, streaming/bistreaming is acceptable");
DEFINE_string(function, "input", "set function, input/shuffle is acceptable");
// File system related
DEFINE_string(user, "", "set username to FS, empty means default");
DEFINE_string(password, "", "set password to FS, empty only when username is empty");
DEFINE_string(host, "", "set host of FS, overwritten by full address");
DEFINE_string(port, "", "set port of FS, overwritten by full address");
DEFINE_string(type, "", "set input FS type, overwritten by full address");
DEFINE_string(address, "", "set address, must be full address when absent host and port");

// Input function parameters
DEFINE_int64(offset, 0, "for input, the start offset of input file to process");
DEFINE_int64(length, 1024, "for input, the length of input to process");
DEFINE_string(format, "text", "for input, input file format, text/seq is acceptable");
DEFINE_bool(nline, false, "for input, switch n-line mode");

// Shuffle function parameters
DEFINE_int32(phase, 0, "for shuffle, the phase that current minion is belong");
DEFINE_int32(no, 0, "for shuffle, the number of current minion");
DEFINE_int32(attempt, 0, "for shuffle, the attempt of current minion");
DEFINE_int32(total, 0, "for shuffle, the total number of previous phase");
DEFINE_int32(pile_scale, 0, "for shuffle, the number of map's output that one pile contains");

static FileType type = kInfHdfs;

static File::Param FillParam() {
    File::Param param;
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
    return param;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baidu::common::SetLogFile(GetLogName("./input.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./input.warning").c_str());
    if (FLAGS_address.empty()) {
        LOG(baidu::WARNING, "please offer a valid address");
        return -1;
    }
    Inlet* parser = NULL;
    const File::Param& param = FillParam();
    if (FLAGS_function == "input") {
        parser = new SourceInlet(type, FLAGS_address, param);
    } else if (FLAGS_function == "shuffle") {
        if (FLAGS_total == 0) {
            LOG(baidu::WARNING, "total number of previous phase is needed in shuffle function");
            return -1;
        }
        parser = new ShuffleInlet(type, FLAGS_address, param);
    } else {
        LOG(baidu::WARNING, "unfamiliar function: %s", FLAGS_function.c_str());
        return -1;
    }
    if (parser == NULL) {
        LOG(baidu::WARNING. "fail to create inlet parser");
        return -1;
    }
    boost::scoped_ptr<Inlet> inlet_guard(parser);
    return parser->Flow();
}

