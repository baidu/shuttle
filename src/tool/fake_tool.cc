#include "proto/minion.pb.h"
#include "proto/master.pb.h"

#define STRIP_FLAG_HELP 1
#include <gflags/gflags.h>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include "common/rpc_client.h"

DEFINE_string(e, "0.0.0.0:0", "endpoint for master/minion");
DEFINE_string(j, "", "job id");
DEFINE_int32(n, 0, "node id");
DEFINE_int32(t, -1, "task id");
DEFINE_int32(a, -1, "attempt id");
DEFINE_string(s, "", "task state");
DEFINE_bool(v, false, "verbose mode");
DEFINE_bool(h, false, "show help info");
DECLARE_bool(help);

const std::string helper = "Faker - fake a master or a minion\n"
    "Usage: faker command [options]\n\n"
    "Command:\n"
    "  ask      ask master for a brand new task\n"
    "  return   return a task to master with certain state\n"
    "  master   change to fake master and server minions\n\n"
    "Options:\n"
    "  -h, --help  show the help info in different commands\n";

static int AskTask() {
    static const std::string helper = "Faker - fake a master or a minion\n"
        "ask: ask master for a brand new task\n\n"
        "Options:\n"
        "  -e <endpoint>  endpoint of master\n"
        "  -j <job id>    job id to get from master\n"
        "  -n <node id>   node id to get from master\n"
        "  -v             verbose, output job description string\n"
        "  -h, --help     show this help info\n";
    if (FLAGS_h || FLAGS_help) {
        std::cerr << helper;
        return 1;
    }
    if (FLAGS_e.find_first_of(":") == std::string::npos) {
        std::cerr << "faker: endpoint is not valid" << std::endl;
        return -1;
    }
    baidu::shuttle::RpcClient rpc;
    baidu::shuttle::Master_Stub* stub = NULL;
    rpc.GetStub(FLAGS_e, &stub);
    boost::scoped_ptr<baidu::shuttle::Master_Stub> stub_guard(stub);
    baidu::shuttle::AssignTaskRequest request;
    baidu::shuttle::AssignTaskResponse response;
    request.set_jobid(FLAGS_j);
    request.set_node(FLAGS_n);
    request.set_endpoint("0.0.0.0:0");
    if (!rpc.SendRequest(stub, &baidu::shuttle::Master_Stub::AssignTask,
            &request, &response, 5, 1)) {
        std::cerr << "faker: cannot reach master" << std::endl;
        return -2;
    }
    if (response.status() != baidu::shuttle::kOk) {
        std::cout << "faker received " << baidu::shuttle::Status_Name(response.status())
            << " and now quit" << std::endl;
        return 0;
    }
    std::cout << "faker received a task:" << std::endl;
    std::cout << "  node: " << response.task().node() << std::endl;
    std::cout << "  task: " << response.task().task_id() << std::endl;
    std::cout << "  attempt: " << response.task().attempt_id() << std::endl;
    std::cout << "  input:" << std::endl;
    std::cout << "    file: " << response.task().input().input_file() << std::endl;
    std::cout << "    offset: " << response.task().input().input_offset() << std::endl;
    std::cout << "    size: " << response.task().input().input_size() << std::endl;
    if (FLAGS_v) {
        std::cout << response.job().DebugString() << std::endl;
    }
    return 0;
}

static int ReturnTask() {
    static const std::string helper = "Faker - fake a master or a minion\n"
        "ask: ask master for a brand new task\n\n"
        "Options:\n"
        "  -e <endpoint>   endpoint of master\n"
        "  -j <job id>     job id to return to master\n"
        "  -n <node id>    node id to return to master\n"
        "  -t <task id>    task id to return to master\n"
        "  -a <attempt id> attempt id to return to master\n"
        "  -s <state>      state to return to master\n"
        "  -h, --help      show this help info\n";
    if (FLAGS_h || FLAGS_help) {
        std::cerr << helper;
        return 1;
    }
    if (FLAGS_e.find_first_of(":") == std::string::npos) {
        std::cerr << "faker: endpoint is not valid" << std::endl;
        return -1;
    }
    baidu::shuttle::RpcClient rpc;
    baidu::shuttle::Master_Stub* stub = NULL;
    rpc.GetStub(FLAGS_e, &stub);
    boost::scoped_ptr<baidu::shuttle::Master_Stub> stub_guard(stub);
    baidu::shuttle::FinishTaskRequest request;
    baidu::shuttle::FinishTaskResponse response;
    request.set_jobid(FLAGS_j);
    request.set_node(FLAGS_n);
    request.set_task_id(FLAGS_t);
    request.set_attempt_id(FLAGS_a);
    baidu::shuttle::TaskState state = baidu::shuttle::kTaskUnknown;
    if (FLAGS_s == "f" || FLAGS_s == "fail") {
        state = baidu::shuttle::kTaskFailed;
    } else if (FLAGS_s == "k" || FLAGS_s == "kill") {
        state = baidu::shuttle::kTaskKilled;
    } else if (FLAGS_s == "c" || FLAGS_s == "completed") {
        state = baidu::shuttle::kTaskCompleted;
    } else if (FLAGS_s == "cancel") {
        state = baidu::shuttle::kTaskCanceled;
    } else if (FLAGS_s == "e" || FLAGS_s == "exist") {
        state = baidu::shuttle::kTaskMoveOutputFailed;
    } else {
        std::cerr << "faker: state string is not accepted" << std::endl;
        return -1;
    }
    request.set_task_state(state);
    request.set_endpoint("0.0.0.0:0");
    if (!rpc.SendRequest(stub, &baidu::shuttle::Master_Stub::FinishTask,
            &request, &response, 5, 1)) {
        std::cerr << "faker: cannot reach master" << std::endl;
        return -2;
    }
    std::cout << "faker finish a task " << FLAGS_j << " - <" << FLAGS_n
        << ", " << FLAGS_t << ", " << FLAGS_a << ">: "
        << baidu::shuttle::Status_Name(response.status()) << std::endl;
    return 0;
}

int main(int argc, char** argv) {
    google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
    if (argc < 2) {
        if (FLAGS_h || FLAGS_help) {
            std::cerr << helper;
            return 1;
        } else {
            std::cerr << "faker: I need your command, sir" << std::endl;
            return -1;
        }
    }
    std::string command = argv[1];
    if (command == "ask") {
        return AskTask();
    } else if (command == "return") {
        return ReturnTask();
    } else if (command == "master") {
    }
    if (FLAGS_h || FLAGS_help) {
        std::cerr << helper;
        return 1;
    }
    std::cerr << "faker: I don't understand your command, sir" << std::endl;
    return -1;
}

