#ifndef _BAIDU_SHUTTLE_CONNECTOR_H_
#define _BAIDU_SHUTTLE_CONNECTOR_H_
#include <string>
#include <ctime>

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
    std::string TimeString(time_t time) {
        static char buf[32] = { 0 };
        ::strftime(buf, 32, "%Y-%m-%d %H:%M:%S", ::localtime(&time));
        return buf;
    }
    static const char* state_string[];
private:
    Configuration* config_;
    Shuttle* sdk_;
};

}
}

#endif

