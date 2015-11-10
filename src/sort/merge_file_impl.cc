#include "sort_file.h"
#include <boost/bind.hpp>
#include <logging.h>

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;

namespace baidu {
namespace shuttle {

const static int sParallelLevel = 12;

MergeFileReader::~MergeFileReader() {
    std::vector<SortFileReader*>::iterator it;
    for (it = readers_.begin(); it != readers_.end(); it++) {
        delete (*it);
    }
}

void MergeFileReader::AddReader(const std::string& file_name, FileSystem::Param param, 
                                FileType file_type, Status* status) {
    Status st;
    SortFileReader* reader = SortFileReader::Create(file_type, &st);
    if (st == kOk) {
        st = reader->Open(file_name, param);
    }
    if (st != kOk) {
        {
            MutexLock lock(&mu_);
            *status = st;
            err_file_ = file_name;
        }
        LOG(WARNING, "failed to open %s, status: %s", 
            file_name.c_str(), Status_Name(st).c_str());
        return;
    } else {
        MutexLock lock(&mu_);
        readers_.push_back(reader);
    }
}

Status MergeFileReader::Open(const std::vector<std::string>& files, 
                             FileSystem::Param param,
                             FileType file_type) {
    if (files.size() == 0) {
        return kInvalidArg;
    }
    Status status = kOk;
    Status* st = new Status();
    ThreadPool pool(sParallelLevel);
    std::vector<std::string>::const_iterator it;
	LOG(INFO, "wait for #%d readers open", files.size());
    for (it = files.begin(); it != files.end(); it++) {
        const std::string& file_name = *it;
        pool.AddTask(boost::bind(&MergeFileReader::AddReader, this, file_name, param, file_type, st)); 
    }
    pool.Stop(true);
    LOG(INFO, "wait file open done");
    status = *st; 
    delete st;
    return status;
}

void MergeFileReader::CloseReader(SortFileReader* reader, Status* st) {
	Status status = reader->Close();
	if (status != kOk) {
		MutexLock lock(&mu_);
		*st = status;
		err_file_ = reader->GetFileName();
	}
}

Status MergeFileReader::Close() {
    std::vector<SortFileReader*>::iterator it;
    Status status = kOk;
	Status* st = new Status();
	ThreadPool pool(sParallelLevel);
	LOG(INFO, "wait #%d readers close", readers_.size());
    for (it = readers_.begin(); it != readers_.end(); it++) {
		SortFileReader* reader = *it;
		pool.AddTask(boost::bind(&MergeFileReader::CloseReader, this, reader, st));
    }
	pool.Stop(true);
	LOG(INFO, "wait readers close done");
	status = *st;
	delete st;
    return status;
}

void MergeFileReader::AddIter(std::vector<SortFileReader::Iterator*>* iters,
                              SortFileReader* reader,
                              const std::string& start_key,
                              const std::string& end_key) {
    SortFileReader::Iterator* it = reader->Scan(start_key, end_key);
    {
        MutexLock lock(&mu_);
        iters->push_back(it);
    }
}

SortFileReader::Iterator* MergeFileReader::Scan(const std::string& start_key, const std::string& end_key) {
    std::vector<SortFileReader::Iterator*>* iters = new std::vector<SortFileReader::Iterator*>();
    std::vector<SortFileReader*>::iterator it;
    ThreadPool pool(sParallelLevel);
	LOG(INFO, "wait for iterators init...");
    for (it = readers_.begin(); it != readers_.end(); it++) {
        SortFileReader * const& reader = *it;
        pool.AddTask(boost::bind(&MergeFileReader::AddIter, this, iters, reader, start_key, end_key));
    }
	pool.Stop(true);
    LOG(INFO, "all iterators done. #%d", iters->size());
    MergeIterator* merge_it = new MergeIterator(*iters, this);
    delete iters;
    return merge_it;
}

MergeFileReader::MergeIterator::MergeIterator(const std::vector<SortFileReader::Iterator*>& iters,
                                              MergeFileReader* reader) {
    merge_reader_ = reader;
    std::vector<SortFileReader::Iterator*>::const_iterator it;
    status_ = kOk;
    int offset = 0;
    for (it = iters.begin(); it != iters.end(); it++) {
        SortFileReader::Iterator * const& reader_it = *it;
        bool drained = false;
        if (!reader_it->Done()) {
            queue_.push(MergeItem(reader_it->Key(), reader_it->Value(), offset));
            iters_.push_back(reader_it);
            offset++;
        } else {
            drained = true;
        }
        if (reader_it->Error() != kOk) {
            status_ = reader_it->Error();
            merge_reader_->err_file_ = reader_it->GetFileName();
        }
        if (drained) {
            delete reader_it;
        }
    }
    if (status_ == kOk && !queue_.empty()) {
        key_ = queue_.top().key_;
        value_ = queue_.top().value_;
    }
}

MergeFileReader::MergeIterator::~MergeIterator() {
    std::vector<SortFileReader::Iterator*>::iterator it;
    for (it = iters_.begin(); it != iters_.end(); it++) {
        SortFileReader::Iterator * const& reader_it = *it;
        delete reader_it;
    }
}

bool MergeFileReader::MergeIterator::Done() {
    return queue_.empty();
}

void MergeFileReader::MergeIterator::Next() {
    if (queue_.empty()) {
        return;
    }
    const MergeItem& top = queue_.top();
    int offset = top.it_offset_;
    SortFileReader::Iterator* reader_it = iters_[offset];
    reader_it->Next();
    queue_.pop();
    if (!reader_it->Done()) {
        queue_.push(MergeItem(reader_it->Key(), reader_it->Value(), offset));
    }
    if (reader_it->Error() != kOk && reader_it->Error() != kNoMore) {
        status_ = reader_it->Error();
        merge_reader_->err_file_ = reader_it->GetFileName();
        LOG(WARNING, "failed to call next of %s, %s", 
            merge_reader_->err_file_.c_str(), Status_Name(status_).c_str());
    }
    if (!queue_.empty()) {
        key_ = queue_.top().key_;
        value_ = queue_.top().value_;
    }
}

}
}

