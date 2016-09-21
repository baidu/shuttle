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
    InternalOutlet() { }
    virtual ~InternalOutlet() { }

    virtual int Collect();
public:
    FileType type_;
    File::Param param_;
    std::string pipe_;
    std::string work_dir_;
    std::string partition_;
    std::string separator_;
    int key_fields_;
    int partition_fields_;
    int dest_num_;
private:
    Partitioner* GetPartitioner();
};

class ResultOutlet : public Outlet {
public:
    ResultOutlet() : fileformat_(kPlainText), multiplex_(false), textoutput_(true) { }
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
public:
    FileType type_;
    File::Param param_;
    std::string pipe_;
    std::string work_dir_;
    std::string format_;
    int no_;
private:
    bool PrepareOutputFiles();
    bool WriteToOutput(const std::string& key, const std::string& value);

    FormattedFile* GetOutputFile(int no);
private:
    std::vector<FormattedFile*> output_pool_;
    FileFormat fileformat_;
    std::string filename_;
    bool multiplex_;
    bool textoutput_;
};

}
}

#endif

