#include <gflags/gflags.h>
#include <boost/scoped_ptr.hpp>
#include <cstdlib>
#include "logging.h"
#include "minion/output/outlet.h"
#include "minion/common/log_name.h"
#include "common/file.h"

using namespace baidu::shuttle;

// Basic parameters
DEFINE_string(pipe, "streaming", "user input/output type, streaming/bistreaming is acceptable");
DEFINE_string(function, "echo", "set function, echo/sort is acceptable");
// File system related
DEFINE_string(user, "", "set username to FS, empty means default");
DEFINE_string(password, "", "set password to FS, empty only when username is empty");
DEFINE_string(host, "", "set host of FS, overwritten by full address");
DEFINE_string(port, "", "set port of FS, overwritten by full address");
DEFINE_string(type, "", "set input FS type, overwritten by full address");
DEFINE_string(address, "", "set work dir, must be full address when absent host and port");
// For sort function, partition related
DEFINE_string(partitioner, "keyhash", "partitioner type, keyhash/inthash is acceptable");
DEFINE_string(separator, "\t", "separator to split record");
DEFINE_int32(key_fields, 1, "number of key fields");
DEFINE_int32(partition_fields, 1, "number of partition fields");
DEFINE_int32(dest_num, 0, "number of next phase");
// For echo function
DEFINE_string(format, "text", "set output format, text/seq/multiple is acceptable");
DEFINE_int32(no, 0, "set the number of current minion");

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

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baidu::common::SetLogFile(GetLogName("./output.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./output.warning").c_str());

    if (FLAGS_address.empty()) {
        LOG(baidu::WARNING, "please offer a valid address");
        return -1;
    }
    Outlet* saver = NULL;
    if (FLAGS_function == "echo") {
        ResultOutlet* outlet = new ResultOutlet();
        FillParam(outlet->param_);
        outlet->type_ = type;
        outlet->pipe_ = FLAGS_pipe;
        outlet->work_dir_ = FLAGS_address;
        outlet->format_ = FLAGS_format;
        outlet->no_ = FLAGS_no;
        saver = outlet;
    } else if (FLAGS_function == "sort") {
        InternalOutlet* outlet = new InternalOutlet();
        FillParam(outlet->param_);
        outlet->type_ = type;
        outlet->pipe_ = FLAGS_pipe;
        outlet->work_dir_ = FLAGS_address;
        outlet->partition_ = FLAGS_partitioner;
        outlet->separator_ = FLAGS_separator;
        outlet->key_fields_ = FLAGS_key_fields;
        outlet->partition_fields_ = FLAGS_partition_fields;
        if (FLAGS_dest_num == 0) {
            LOG(baidu::WARNING, "total number of next phase is needed in shuffle function");
            return -1;
        }
        outlet->dest_num_ = FLAGS_dest_num;
        saver = outlet;
    } else {
        LOG(baidu::WARNING, "unfamiliar function: %s", FLAGS_function.c_str());
        return -1;
    }
    boost::scoped_ptr<Outlet> oulet_guard(saver);
    return saver->Collect();
}
