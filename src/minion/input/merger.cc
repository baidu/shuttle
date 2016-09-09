#include "merger.h"

#include <boost/bind.hpp>
#include "thread_pool.h"
#include "mutex.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

Merger::Merger(const std::vector<FormattedFile*> files) {
    for (std::vector<FormattedFile*>::const_iterator it = files.begin();
            it != files.end(); ++it) {
        FormattedFile* fp = *it;
        if (fp->Error() == kOk) {
            sortfiles_.push_back(fp);
        }
    }
}

Scanner::Iterator* Merger::Scan(const std::string& start_key, const std::string& end_key) {
    std::vector<Scanner::Iterator*> scanning;
    Mutex mu;
    ThreadPool tp(PARALLEL_LEVEL);
    LOG(DEBUG, "initiate all internal files...");
    for (std::vector<FormattedFile*>::iterator it = sortfiles_.begin();
            it != sortfiles_.end(); ++it) {
        FormattedFile* fp = *it;
        tp.AddTask(boost::bind(&Merger::AddProvedIter, this,
                start_key, end_key, fp, &scanning, &mu));
    }
    tp.Stop(true);
    LOG(DEBUG, "all internal files are ready");
    return new Iterator(scanning);
}

void Merger::AddProvedIter(const std::string& start_key, const std::string& end_key,
        FormattedFile* fp, std::vector<Scanner::Iterator*>* to_be_scanned, Mutex* vec_mu) {
    if (to_be_scanned == NULL) {
        return;
    }
    Scanner* scanner = Scanner::Get(fp, kInternalScanner);
    Scanner::Iterator* it = scanner->Scan(start_key, end_key);
    if (it != NULL && it->Error() == kOk) {
        MutexLock lock(vec_mu);
        to_be_scanned->push_back(it);
    } else {
        LOG(WARNING, "scanner report error: %s", it ? it->GetFileName().c_str() : "NULL");
        if (it != NULL) {
            delete it;
        }
    }
    delete scanner;
}

Merger::Iterator::Iterator(const std::vector<Scanner::Iterator*>& iters) {
    status_ = kOk;
    int offset = 0;
    for (std::vector<Scanner::Iterator*>::const_iterator it = iters.begin();
            it != iters.end(); ++it) {
        Scanner::Iterator* iter = *it;
        if (!iter->Done()) {
            queue_.push(MergeItem(iter->Key(), iter->Value(), offset));
            iters_.push_back(iter);
            ++offset;
        } else if (iter->Error() == kNoMore) {
            delete iter;
        } else {
            status_ = iter->Error();
            err_file_ = iter->GetFileName();
            LOG(WARNING, "fail to merge %s, return %s",
                    err_file_.c_str(), Status_Name(status_).c_str());
            delete iter;
            break;
        }
    }
    if (!queue_.empty()) {
        key_ = queue_.top().key;
        value_ = queue_.top().value;
    }
}

Merger::Iterator::~Iterator() {
    for (std::vector<Scanner::Iterator*>::iterator it = iters_.begin();
            it != iters_.end(); ++it) {
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
    Scanner::Iterator* it = iters_[offset];
    it->Next();
    queue_.pop();
    if (!it->Done()) {
        queue_.push(MergeItem(it->Key(), it->Value(), offset));
    } else if (it->Error() != kNoMore) {
        status_ = it->Error();
        err_file_ = it->GetFileName();
        LOG(WARNING, "fail to call next of %s, return %s",
                err_file_.c_str(), Status_Name(status_).c_str());
    }
    if (!queue_.empty()) {
        key_ = queue_.top().key;
        value_ = queue_.top().value;
    } else {
        status_ = kNoMore;
    }
}

}
}

