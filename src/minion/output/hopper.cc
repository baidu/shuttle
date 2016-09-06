#include "hopper.h"

#include "common/fileformat.h"
#include <algorithm>
#include <cstdio>

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

Hopper::Hopper(const std::string& work_dir) : file_no_(0), work_dir_(work_dir) {
    // TODO
}

// Flushing in Hopper leads to writing result file
Status Hopper::Flush() {
    if (mem_table_.empty()) {
        return kNoMore;
    }
    // Prepare output file
    File::Param param;
    // TODO Fill param
    FormattedFile* fp = FormattedFile::Create(kInfHdfs, kInternalSortedFile, param);
    if (fp == NULL) {
        LOG(WARNING, "fail to get file handler, connection may be interrupted");
        return kOpenFileFail;
    }
    char buf[4096];
    snprintf(file_name, sizeof(file_name), "%s/%d.sort",
            work_dir_.c_str(), file_no_);
    if (fp->Open(file_name, kWriteFile, param)) {
        LOG(WARNING, "fail to open file to flush data: %s", file_name);
        return kOpenFileFail;
    }
    // Sort records
    std::sort(mem_table_.begin(), mem_table_.end(), HopperItemLess());
    // Write records as sorted file
    Status status = kOk;
    for (std::vector<EmitItem*>::iterator it = mem_table_.begin();
            it != mem_table_.end(); ++it) {
        HopperItem* cur = static_cast<HopperItem*>(*it);
        snprintf(buf, sizeof(buf), "%05d", cur->dest);
        std::string key(buf);
        key += "\t";
        key += cur->key;
        if (!fp->WriteRecord(key, cur->value)) {
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

