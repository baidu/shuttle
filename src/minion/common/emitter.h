#ifndef _BAIDU_SHUTTLE_EMITTER_H_
#define _BAIDU_SHUTTLE_EMITTER_H_
#include <string>
#include <vector>
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

class EmitItem {
public:
    virtual size_t Size() = 0;

    virtual EmitItem* GetCopy() = 0;
};

class Emitter {
public:
    Emitter();
    virtual ~Emitter() { Reset(); }
    virtual Status Emit(EmitItem* const& item);
    virtual void Reset();
    virtual void SetMaxSize(size_t size) {
        max_size_ = size;
    }

    virtual Status Flush() = 0;
private:
    size_t max_size_;
    size_t cur_size_;
    std::vector<EmitItem*> mem_table_;
};

}
}

#endif

