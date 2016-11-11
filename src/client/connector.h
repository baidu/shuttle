#ifndef _BAIDU_SHUTTLE_CONNECTOR_H_
#define _BAIDU_SHUTTLE_CONNECTOR_H_
#include <string>

namespace baidu {
namespace shuttle {

class Configuration;
class Shuttle;

class ShuttleConnector {
public:
    ShuttleConnector(Configuration* config);
    ~ShuttleConnector() { }

    int Submit();
    int Update();
    int Kill();
    int List();
    int Status();
    int Monitor();
private:
    std::string GetMasterAddr();
private:
    Configuration* config_;
    Shuttle* sdk_;
};

}
}

#endif

