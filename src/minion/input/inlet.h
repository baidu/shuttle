#ifndef _BAIDU_SHUTTLE_INLET_H_
#define _BAIDU_SHUTTLE_INLET_H_
#include <string>
#include <vector>
#include <set>
#include "common/file.h"

namespace baidu {
namespace shuttle {

class Inlet {
public:
    virtual int Flow() = 0;

    virtual ~Inlet() { }
};

class SourceInlet : public Inlet {
public:
    SourceInlet(FileType type, const std::string& path, const File::Param& param)
            : type_(type), param_(param), file_(path) { }
    virtual ~SourceInlet() { }

    virtual int Flow();
private:
    FileType type_;
    File::Param param_;
    std::string file_;
};

class ShuffleInlet : public Inlet {
public:
    ShuffleInlet(FileType type, const std::string& path, const File::Param& param);
    virtual ~ShuffleInlet() {
        if (fp_ != NULL) {
            delete fp_;
            fp_ = NULL;
        }
    }

    virtual int Flow();
private:
    bool PreMerge(const std::vector<std::string>& files, const std::string& output);
    bool FinalMerge(const std::vector<std::string>& files);
    int PileMerge(const std::vector<int>& pile_list);

    int CheckPileExecution(std::set<int>& done, const std::vector<int>& pile_list);
    bool PrepareSortFiles(std::vector<std::string>& files, int from, int to);
private:
    FileType type_;
    File::Param param_;
    std::string work_dir_;
    int pile_scale_;

    File* fp_;
};

}
}

#endif

