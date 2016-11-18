#ifndef _BAIDU_SHUTTLE_CONFIG_H_
#define _BAIDU_SHUTTLE_CONFIG_H_
#include <string>
#include <vector>
#include <iostream>
#include "sdk/shuttle.h"

namespace baidu {
namespace shuttle {

class Configuration {
public:
    Configuration() { }
    ~Configuration() { }

    int ParseCommandLine(int argc, char** argv);
    int ParseJson(std::istream& is);

    int BuildJobDescription(sdk::JobDescription& job);
    int BuildJson(std::ostream& os);

    std::string Help() const;

    std::string GetConf(const std::string& name) const {
        std::map<std::string, std::string>::const_iterator it = kv_.find(name);
        if (it == kv_.end()) {
            return "";
        }
        return it->second;
    }
    void GetConf(const std::string& name,
            std::vector<std::string>& value) const {
        std::map< std::string, std::vector<std::string> >::const_iterator it
                = multivalue_.find(name);
        if (it == multivalue_.end()) {
            return;
        }
        value = it->second;
    }

    bool AddConf(const std::string& name, const std::string& value) {
        std::map< std::string, std::vector<std::string> >::iterator it
                = multivalue_.find(name);
        if (it == multivalue_.end()) {
            return false;
        }
        it->second.push_back(value);
        return true;
    }
private:
    int64_t ParseMemory(const std::string& memory);
    int32_t ParseNumberWithDefault(const std::string& key, int32_t def_value);
    void FillLegacyNodes();
    void InteractiveGetConfig();
private:
    std::map<std::string, std::string> kv_;
    std::map< std::string, std::vector<std::string> > multivalue_;

    std::vector<sdk::NodeConfig> nodes_;
    std::vector< std::vector<int> > successors_;
};

}
}

#endif

