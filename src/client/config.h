#ifndef _BAIDU_SHUTTLE_CONFIG_H_
#define _BAIDU_SHUTTLE_CONFIG_H_
#include <string>
#include <vector>
#include <boost/program_options.hpp>
#include "sdk/shuttle.h"

namespace baidu {
namespace shuttle {

class Configuration {
public:
    Configuration() { }
    ~Configuration() { }

    int ParseCommandLine(int argc, char** argv);
    std::string Command() const {
        return vars_.count("command") ? vars_["command"].as<std::string>() : "";
    }
    std::string Help() const;
    int BuildJobDescription(sdk::JobDescription& job) const;
private:
    boost::program_options::variables_map vars_;
    std::map<std::string, std::string> conf_;
};

}
}

#endif

