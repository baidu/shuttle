#include "emitter.h"

namespace baidu {
namespace shuttle {

static const size_t default_max_mem_table = 512 << 20;

Emitter::Emitter() : max_size_(default_max_mem_table), cur_size_(0) {
}

Status Emitter::Emit(EmitItem* const& item) {
    EmitItem* copy = item->GetCopy();
    mem_table_.push_back(copy);
    cur_size_ += copy->Size();

    if (cur_size_ < max_size_) {
        return kOk;
    }
    return Flush();
}

void Emitter::Reset() {
    cur_size_ = 0;
    for (std::vector<EmitItem*>::iterator it = mem_table_.begin();
            it != mem_table_.end(); ++it) {
        delete (*it);
    }
    mem_table_.clear();
}

}
}

