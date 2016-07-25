#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <string>

namespace baidu {
namespace shuttle {

void ParseHdfsAddress(const std::string& address, std::string* host, int* port,
                      std::string* path) {
    /* A hdfs path is composed with:
     *   A magic number: `hdfs://`
     *   A server hostname/ip:port format: `localhost:54310`
     *   An absolute path: `/somepath/somefile`
     */
    // string must not contain trailing spaces and enters
    if (!boost::starts_with(address, "hdfs://")) {
        return;
    }
    // len('hdfs://') == 7
    size_t server_path_seperator = address.find_first_of('/', 7);
    const std::string& server = address.substr(7, server_path_seperator - 7);
    size_t last_colon = server.find_last_of(':');
    // address doesn't contain port information
    if (last_colon == std::string::npos) {
        return;
    }
    if (host != NULL) {
        *host = server.substr(0, last_colon);
    }
    if (port != NULL) {
        try {
            *port = boost::lexical_cast<int>(server.substr(last_colon + 1));
        } catch (boost::bad_lexical_cast&) {
            *port = 54310;
        }
    }
    if (path != NULL) {
        *path = address.substr(server_path_seperator);
    }
}

bool PatternMatch(const std::string& origin, const std::string& pattern) {
    const char* str = origin.c_str();
    const char* pat = pattern.c_str();
    const char* cp = NULL;
    const char* mp = NULL;

    while (*str && *pat != '*') {
        if (*pat != *str && *pat != '?') {
            return false;
        }
        ++str;
        ++pat;
    }

    while (*str) {
        if (*pat == '*') {
            if (!*++pat) {
                return true;
            }
            mp = pat;
            cp = str + 1;
        } else if (*pat == *str || *pat == '?') {
            ++pat;
            ++str;
        } else {
            pat = mp;
            str = cp++;
        }
    }

    while (*pat == '*') {
        ++pat;
    }
    return !*pat;
}

}
}

