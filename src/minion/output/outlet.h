#ifndef _BAIDU_SHUTTLE_OUTLET_H_
#define _BAIDU_SHUTTLE_OUTLET_H_
#include "minion/common/streaming.h"
#include <vector>
#include <cstdio>

namespace baidu {
namespace shuttle {

class Outlet {
public:
    virtual int Collect() = 0;

    virtual ~Outlet() { }
protected:
    FormattedFile* GetFileWrapper(FILE* fp, const std::string& pipe);
};

class Partitioner;

class InternalOutlet : public Outlet {
public:
    InternalOutlet(FileType type, const File::Param& param)
            : type_(type), param_(param) { }
    virtual ~InternalOutlet() { }

    virtual int Collect();
private:
    FileType type_;
    File::Param param_;
    Partitioner* GetPartitioner();
};

class ResultOutlet : public Outlet {
public:
    ResultOutlet(FileType type, const File::Param& param) : type_(type), param_(param),
            fileformat_(kPlainText), multiplex_(false), textoutput_(true) { }
    virtual ~ResultOutlet() {
        for (std::vector<FormattedFile*>::iterator it = output_pool_.begin();
                it != output_pool_.end(); ++it) {
            FormattedFile* cur = *it;
            if (cur != NULL) {
                cur->Close();
                delete cur;
            }
        }
    }

    virtual int Collect();
private:
    bool PrepareOutputFiles();
    bool WriteToOutput(const std::string& key, const std::string& value);

    FormattedFile* GetOutputFile(int no);
private:
    FileType type_;
    File::Param param_;

    std::vector<FormattedFile*> output_pool_;
    FileFormat fileformat_;
    std::string filename_;
    bool multiplex_;
    bool textoutput_;
};

}
}

#endif

