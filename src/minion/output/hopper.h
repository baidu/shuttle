#ifndef _BAIDU_SHUTTLE_HOPPER_H_
#define _BAIDU_SHUTTLE_HOPPER_H_
#include "minion/common/emitter.h"
#include "common/file.h"
#include <string>

namespace baidu {
namespace shuttle {

class HopperItem : public EmitItem {
public:
    int dest;
    std::string key;
    std::string record;

    HopperItem() { }
    HopperItem(int dest, const std::string& key, const std::string& record) :
        dest(dest), key(key), record(record) { }

    virtual size_t Size() {
        return sizeof(dest) + key.size() + record.size();
    }
    virtual EmitItem* GetCopy() {
        return new HopperItem(this->dest, this->key, this->record);
    }
};

class Hopper : public Emitter {
public:
    Hopper(const std::string& work_dir, FileType type, const File::Param& param) :
            type_(type), file_no_(0), work_dir_(work_dir), param_(param) {
        if (*work_dir_.rbegin() != '/') {
            work_dir_.push_back('/');
        }
    }
    virtual ~Hopper() { }

    virtual Status Flush();
protected:
    FileType type_;
    int file_no_;
    std::string work_dir_;
    File::Param param_;
};

}
}

#endif

