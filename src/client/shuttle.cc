#include <iostream>
#include <string>
#include <boost/program_options.hpp>
#include "sdk/shuttle.h"
#include "client/config.h"

int main(int argc, char** argv) {
    Configuration config;
    int ret = config.ParseCommandLine(argc, argv);
    if (ret != 0) {
        return ret;
    }
    const std::string command = config.GetConf("command");
    if (command.empty()) {
        std::cerr << "ERROR: please offer a command" << std::endl;
        return -1;
    }
    if (command == "json") {
    } else if (command == "submit") {
    } else if (command == "update") {
    } else if (command == "kill") {
    } else if (command == "list") {
    } else if (command == "status") {
    } else if (command == "monitor") {
    } else {
        std::cerr << "ERROR: " << command << " command is not recognized."
                  << "Please refer to help command" << std::endl;
        return -1;
    }
    return 0;
}

