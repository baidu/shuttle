#ifndef _BAIDU_SHUTTLE_HOPPER_H_
#define _BAIDU_SHUTTLE_HOPPER_H_
#include <string>

#include "minion/common/emitter.h"

namespace baidu {
namespace shuttle {

class HopperItem : public EmitItem {
public:
    int dest;
    std::string key;
    std::string record;
    
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
    // TODO
    Hopper();
    virtual ~Hopper() { }

    virtual Status Flush();
};

}
}

#endif

