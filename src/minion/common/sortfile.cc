#include "sortfile.h"

#include "logging.h"

namespace baidu {
namespace shuttle {

Status SortedRecordReader::Open(const std::string& path, FileSystem::Param& param) {
    LOG(INFO, "try to open: %s", path.c_str());
    path_ = path;
    if (!fs_->Open(path, param, kReadFile)) {
        return kOpenFileFail;
    }
    return kOk;
}

SortedRecordReader::SortedRecordIterator* SortedRecordReader::Scan(
        const std::string& start_key, const std::string& end_key) {
}

}
}

