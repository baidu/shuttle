#include "hopper.h"

#include "common/fileformat.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include "logging.h"

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

// Flushing in Hopper leads to writing result file
Status Hopper::Flush() {
    if (mem_table_.empty()) {
        return kNoMore;
    }
    // Prepare output file
    FormattedFile* fp = FormattedFile::Create(kInfHdfs, kInternalSortedFile, param_);
    if (fp == NULL) {
        LOG(WARNING, "fail to get file handler, connection may be interrupted");
        return kOpenFileFail;
    }
    std::stringstream output_ss;
    output_ss << work_dir_ << file_no_ << ".sort";
    if (fp->Open(output_ss.str(), kWriteFile, param_)) {
        LOG(WARNING, "fail to open file to flush data: %s", output_ss.str().c_str());
        return kOpenFileFail;
    }
    // Sort records
    std::sort(mem_table_.begin(), mem_table_.end(), HopperItemLess());
    // Write records as sorted file
    Status status = kOk;
    for (std::vector<EmitItem*>::iterator it = mem_table_.begin();
            it != mem_table_.end(); ++it) {
        HopperItem* cur = static_cast<HopperItem*>(*it);
        std::stringstream ss;
        ss << std::setw(5) << std::setfill('0') << cur->dest;
        const std::string& key = ss.str();
        if (!fp->WriteRecord(key + "\t" + cur->key, cur->record)) {
            status = kWriteFileFail;
            break;
        }
    }
    if (!fp->Close()) {
        status = kCloseFileFail;
    }
    delete fp;
    // Counter increment to record sort file name
    if (status == kOk) {
        ++file_no_;
    }
    Reset();
    return status;
}

}
}

