#include "hopper.h"

namespace baidu {
namespace shuttle {

struct HopperItemLess {
    bool operator()(EmitItem* const& a, EmitItem* const& b) {
        // Type cast here is safety since this comparator only used in Hopper
        HopperItem* lhs = static_cast<HopperItem*>(a);
        HopperItem* rhs = static_cast<HopperItem*>(b);
        if (lhs->dest < rhs->dest) {
            return true;
        }
        if (lhs->dest > rhs->dest) {
            return false;
        }
        return lhs->key < rhs->key;
    }
};

Hopper::Hopper() {
}

// Flushing in Hopper leads to writing result file
Status Hopper::Flush() {
}

}
}

