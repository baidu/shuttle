#include "merger.h"

#include <boost/bind.hpp>
#include "thread_pool.h"
#include "mutex.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

KVScanner::Iterator* Merger::Scan(const std::string& start_key, const std::string& end_key) {
    std::vector<FormattedFile*> scanning;
    Mutex mu;
    ThreadPool tp(PARALLEL_LEVEL);
    LOG(DEBUG, "initiate all intermit files...");
    for (std::vector<FormattedFile*>::iterator it = sortfiles_.begin();
            it != sortfiles_.end(); ++it) {
        FormattedFile* fp = *it;
        tp.AddTask(boost::bind(&Merger::AddProvedFile, this, start_key, fp, scanning, &mu));
    }
    tp.Stop(true);
    LOG(DEBUG, "all intermit files are ready");
    return new Iterator(scanning, end_key);
}

Status Merger::Open(const std::string& /*path*/, const File::Param& /*param*/) {
    // TODO
    return kNotImplement;
}

Status Merger::Close() {
    // TODO
    return kNotImplement;
}

void Merger::AddProvedFile(const std::string& start_key, FormattedFile* fp,
        std::vector<FormattedFile*>& to_be_scanned, Mutex* vec_mu) {
    if (fp->Locate(start_key)) {
        MutexLock lock(vec_mu);
        to_be_scanned.push_back(fp);
    }
}

Merger::Iterator::Iterator(const std::vector<FormattedFile*>& files, const std::string& end_key) :
        end_key_(end_key) {
    status_ = kOk;
    int offset = 0;
    for (std::vector<FormattedFile*>::const_iterator it = files.begin();
            it != files.end(); ++it) {
        FormattedFile* fp = *it;
        std::string key, value;
        if (fp->ReadRecord(key, value)) {
            queue_.push(MergeItem(key, value, offset));
            sortfiles_.push_back(fp);
            ++offset;
        } else {
            if (fp->Error() != kOk && fp->Error() != kNoMore) {
                err_file_ = fp->GetFileName();
                LOG(WARNING, "fail to merge: %s", err_file_.c_str());
            }
            delete fp;
        }
    }
    if (!queue_.empty()) {
        key_ = queue_.top().key;
        value_ = queue_.top().value;
    }
}

Merger::Iterator::~Iterator() {
    for (std::vector<FormattedFile*>::iterator it = sortfiles_.begin();
            it != sortfiles_.end(); ++it) {
        delete (*it);
    }
}

bool Merger::Iterator::Done() {
    return queue_.empty();
}

void Merger::Iterator::Next() {
    if (queue_.empty()) {
        status_ = kNoMore;
        return;
    }
    const MergeItem& top = queue_.top();
    int offset = top.file_offset;
    FormattedFile* fp = sortfiles_[offset];
    queue_.pop();
    std::string key, value;
    if (fp->ReadRecord(key, value)) {
        queue_.push(MergeItem(key, value, offset));
    } else if (fp->Error() != kOk && fp->Error() == kNoMore) {
        status_ = fp->Error();
        err_file_ = fp->GetFileName();
        LOG(WARNING, "fail to call next of %s, return %s",
                err_file_.c_str(), Status_Name(status_).c_str());
    }
    if (!queue_.empty()) {
        key_ = queue_.top().key;
        value_ = queue_.top().value;
    }
}

}
}

