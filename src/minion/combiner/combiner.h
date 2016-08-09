#ifndef _BAIDU_SHUTTLE_COMBINER_H_
#define _BAIDU_SHUTTLE_COMBINER_H_
#include <string>

#include "minion/common/emitter.h"

namespace baidu {
namespace shuttle {

class CombinerItem : public EmitItem {
public:
    std::string key;
    std::string record;

    CombinerItem(const std::string& key, const std::string& record) :
        key(key), record(record) { }
    
    virtual size_t Size() {
        return key.size() + record.size();
    }

    virtual EmitItem* GetCopy() {
        return new CombinerItem(this->key, this->record);
    }
};

class Combiner : public Emitter {
public:
    Combiner(const std::string& cmd) : user_cmd_(cmd) { }
    virtual ~Combiner() { }

    virtual Status Flush();

private:
    std::string user_cmd_;
};

}
}

#endif

