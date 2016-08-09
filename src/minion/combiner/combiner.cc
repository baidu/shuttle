#include "combiner.h"

namespace baidu {
namespace shuttle {

struct CombinerItemLess {
    bool operator()(EmitItem* const& a, EmitItem* const& b) {
        // Type cast here is safety since this comparator only used in Combiner
        return static_cast<CombinerItem*>(a)->key < static_cast<CombinerItem*>(b)->key;
    }
};

// Flushing in Combiner means invoking user command
Status Combiner::Flush() {
}

}
}

