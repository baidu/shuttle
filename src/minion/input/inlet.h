#ifndef _BAIDU_SHUTTLE_INLET_H_
#define _BAIDU_SHUTTLE_INLET_H_
#include <string>
#include <vector>
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
    SourceInlet() : is_nline_(false), offset_(0), len_(0) { }
    virtual ~SourceInlet() { }

    virtual int Flow();
public:
    File::Param param_;
    std::string type_;
    std::string format_;
    std::string file_;
    std::string pipe_;
    bool is_nline_;
    int64_t offset_;
    int64_t len_;
};

class ShuffleInlet : public Inlet {
public:
    ShuffleInlet() : phase_(0), no_(0), attempt_(0),
                     total_(0), pile_scale_(0), fp_(NULL) { }
    virtual ~ShuffleInlet() {
        if (fp_ != NULL) {
            delete fp_;
            fp_ = NULL;
        }
    }

    virtual int Flow();
public:
    File::Param param_;
    FileType file_type_;
    std::string type_;
    std::string work_dir_;
    std::string pipe_;
    int phase_;
    int no_;
    int attempt_;
    int total_;
    int pile_scale_;
private:
    bool PreMerge(const std::vector<std::string>& files, const std::string& output);
    bool FinalMerge(const std::vector<std::string>& files);
    int PileMerge(const std::vector<int>& pile_list);
private:
    File* fp_;
};

}
}

#endif

