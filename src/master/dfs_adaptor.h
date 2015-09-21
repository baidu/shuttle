#ifndef _BAIDU_SHUTTLE_DFS_ADAPTOR_H_
#define _BAIDU_SHUTTLE_DFS_ADAPTOR_H_
#include <string>
#include <vector>
#include <stdlib.h>

#include "hdfs.h"

namespace baidu {
namespace shuttle {

class DfsAdaptor {
public:
    DfsAdaptor(const std::string& dfs_url, const char* options);
    DfsAdaptor(const std::string& dfs_server);
    virtual ~DfsAdaptor();

    bool Open(const char* path, const char* options);
    void Close();
    int Read(void* buffer, size_t size);
    int Write(void* buffer, size_t size);
    void Flush();

    // generic file-system operation
    bool ListDirectory(const std::string& dir, std::vector<std::string>& files);

private:
    void ParseHdfsPath(const std::string& path);

private:
    hdfsFS fs_;
    hdfsFile current_file_;
    int current_options_;
    std::string dfs_server_;
    std::string dfs_path_;

};

}
}

#endif

