#include "inlet.h"

#include "minion/input/merger.h"
#include "common/fileformat.h"
#include "common/scanner.h"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <cmath>
#include <cstdlib>
#include "logging.h"

namespace baidu {
namespace shuttle {

int SourceInlet::Flow() {
    // Prepare scanner
    FileFormat format = kPlainText;
    if (format_ == "text") {
        format = kPlainText;
    } else if (format_ == "seq") {
        format = kInfSeqFile;
    } else {
        LOG(WARNING, "unknown file format: %s", format_.c_str());
        return -1;
    }
    FormattedFile* fp = FormattedFile::Create(type_, format, param_);
    if (!fp->Open(file_, kReadFile, param_)) {
        LOG(WARNING, "fail to open: %s", file_.c_str());
        return 1;
    }
    Scanner* scanner = Scanner::Get(fp, kInputScanner);
    Scanner::Iterator* it = scanner->Scan(offset_, len_);

    /*
     * The record send to user app is organized as following:
     * streaming:
     *   plain text:
     *     block: {line}\n
     *     nline: {line number}\t{line}\n
     *   seq file:
     *     binary_record({ key, value })
     * bistreaming:
     *   plain text:
     *     block: binary_record({ offset, line })
     *     nline: binary_record({ line number, line })
     *   seq file:
     *     binary_record({ key, value })
     */
    int no = 0;
    std::stringstream offset_ss;
    offset_ss << offset_;
    for (; !it->Done(); it->Next()) {
        std::string record;
        if (format == kInfSeqFile) {
            record = FormattedFile::BuildRecord(kInfSeqFile, it->Key(), it->Value());
        } else { // kPlainText ensured
            std::string key;
            FileFormat record_format = kPlainText;
            std::stringstream no_ss;
            no_ss << no++;
            if (pipe_ == "streaming") {
                record_format = kPlainText;
                if (is_nline_) {
                    key = no_ss.str();
                }
            } else if (pipe_ == "bistreaming") {
                record_format = kInfSeqFile;
                if (is_nline_) {
                    key = no_ss.str();
                } else {
                    key = offset_ss.str();
                }
            }
            // Use "{line}\n" record format by default, if no proper pipe is specified
            record = FormattedFile::BuildRecord(record_format, key, it->Value());
        }
        std::cout << record;
    }
    if (it->Error() != kNoMore) {
        LOG(WARNING, "error occurs when reading: ", file_.c_str());
        return 1;
    }

    delete it;
    delete scanner;
    fp->Close();
    delete fp;
    return 0;
}

int ShuffleInlet::Flow() {
    srand(time(NULL));
    // Create object-wide file pointer
    fp_ = File::Create(type_, param_);
    if (fp_ == NULL) {
        LOG(WARNING, "fail to create file pointer");
        return -1;
    }
    if (pile_scale_ == 0) {
        pile_scale_ = std::min((int32_t)ceil(sqrt(total_)), 300);
        if (pile_scale_ < 100) {
            pile_scale_ = std::max(pile_scale_, 10);
        }
    }
    // Add trailing '/' in work dir
    if (*work_dir_.rbegin() != '/') {
        work_dir_.push_back('/');
    }
    // Prepare pile list and shuffle to avoid collision
    int pile_num = (int)ceil((double)total_ / pile_scale_);
    std::vector<int> pile_list;
    for (int i = 0; i < pile_num; ++i) {
        pile_list.push_back(i);
    }
    std::random_shuffle(pile_list.begin(), pile_list.end());
    // Pre-merge to create several piles
    int old_num = pile_num;
    if ((pile_num = PileMerge(pile_list)) < old_num) {
        LOG(WARNING, "error when pre-merging sortfiles");
        return 1;
    }
    std::vector<std::string> pile_names;
    for (int i = 0; i < pile_num; ++i) {
        std::stringstream ss;
        ss << work_dir_ << i << ".pile";
        pile_names.push_back(ss.str());
    }
    // Merge piles to final result and write to stdout
    return FinalMerge(pile_names) ? 0 : 1;
}

// Use do...while(0) to achieve 'finally' function
bool ShuffleInlet::PreMerge(const std::vector<std::string>& files,
        const std::string& output) {
    // Open a sorted file to write pre-merged data
    FormattedFile* writer = FormattedFile::Create(type_, kInternalSortedFile, param_);
    if (writer == NULL) {
        LOG(WARNING, "empty output file pointer: %s", output.c_str());
        return false;
    }
    if (!writer->Open(output, kWriteFile, param_)) {
        LOG(WARNING, "fail to open output file: %s", output.c_str());
        return false;
    }
    bool ok = true;
    // Writer is opened
    do {
        // Open sorted files for merger
        std::vector<FormattedFile*> merge_files;
        for (std::vector<std::string>::const_iterator it = files.begin();
                it != files.end(); ++it) {
            FormattedFile* fp = FormattedFile::Create(type_, kInternalSortedFile, param_);
            if (fp == NULL) {
                continue;
            }
            if (!fp->Open(*it, kReadFile, param_)) {
                LOG(WARNING, "fail to open: %s", it->c_str());
                delete fp;
                continue;
            }
            merge_files.push_back(fp);
        }
        if (merge_files.empty()) {
            LOG(WARNING, "no valid pre-merge file");
            ok = false;
            break;
        }
        // Input files are now opened
        do {
            // Build merger to get iterator
            Merger merger(merge_files);
            Scanner::Iterator* it = merger.Scan(
                    Scanner::SCAN_KEY_BEGINNING, Scanner::SCAN_ALL_KEY);
            if (it == NULL) {
                LOG(WARNING, "fail to get scan iterator");
                ok = false;
                break;
            }
            if (it->Error() != kOk && it->Error() != kNoMore) {
                LOG(WARNING, "fail to scan, error file: %s", it->GetFileName().c_str());
                ok = false;
                delete it;
                break;
            }
            // Iterate over merger data and write to output sorted file
            for (; !it->Done(); it->Next()) {
                if (!writer->WriteRecord(it->Key(), it->Value())) {
                    LOG(WARNING, "fail to write (%s, %s): %s",
                            it->Key().c_str(), it->Value().c_str(), output.c_str());
                    ok = false;
                    break;
                }
            }
            if (it->Error() != kOk && it->Error() != kNoMore) {
                LOG(WARNING, "fail to scan: %s", it->GetFileName().c_str());
                ok = false;
            }
            delete it;
        } while(0);
        // Input files cleaning up
        for (std::vector<FormattedFile*>::iterator it = merge_files.begin();
                it != merge_files.end(); ++it) {
            (*it)->Close();
            delete (*it);
        }
    } while(0);
    // Writer var cleaning up
    writer->Close();
    delete writer;
    return ok;
}

bool ShuffleInlet::FinalMerge(const std::vector<std::string>& files) {
    // Open sorted files for merger
    std::vector<FormattedFile*> merge_files;
    for (std::vector<std::string>::const_iterator it = files.begin();
            it != files.end(); ++it) {
        FormattedFile* fp = FormattedFile::Create(type_, kInternalSortedFile, param_);
        if (fp == NULL) {
            continue;
        }
        if (!fp->Open(*it, kReadFile, param_)) {
            LOG(WARNING, "fail to open: %s", it->c_str());
            delete fp;
            continue;
        }
        merge_files.push_back(fp);
    }
    if (merge_files.empty()) {
        LOG(WARNING, "no valid pre-merge file");
        return false;
    }
    bool ok = true;
    do {
        std::stringstream ss;
        ss << std::setw(5) << std::setfill('0') << no_;
        // Build merger to get iterator
        Merger merger(merge_files);
        // Scan all the records of the same no
        Scanner::Iterator* it = merger.Scan(ss.str(), ss.str() + "\xff");
        if (it == NULL) {
            LOG(WARNING, "fail to get scan iterator");
            ok = false;
            break;
        }
        if (it->Error() != kOk && it->Error() != kNoMore) {
            LOG(WARNING, "fail to scan, error file: %s", it->GetFileName().c_str());
            ok = false;
            break;
        }
        // Iterate over merger data and write to stdout for user cmd
        for (; !it->Done(); it->Next()) {
            if (pipe_ == "streaming") {
                std::cout << it->Value() << std::endl;
            } else if (pipe_ == "bistreaming") {
                std::cout << it->Value();
            } else {
                // Output line with endl by default, if no proper pipe is specified
                std::cout << it->Value() << std::endl;
            }
        }
        if (it->Error() != kOk && it->Error() != kNoMore) {
            LOG(WARNING, "fail to scan: %s", it->GetFileName().c_str());
            ok = false;
        }
    } while(0);
    // Pre-merged files cleaning up
    for (std::vector<FormattedFile*>::iterator it = merge_files.begin();
            it != merge_files.end(); ++it) {
        (*it)->Close();
        delete (*it);
    }
    return ok;
}

int ShuffleInlet::PileMerge(const std::vector<int>& pile_list) {
    // Prepare temp output dir, move piles to formal position when finished
    std::stringstream temp_dir_ss;
    temp_dir_ss << work_dir_ << "pile_" << no_ << "_" << attempt_;
    const std::string& temp_dir = temp_dir_ss.str();
    if (!fp_->Mkdir(temp_dir)) {
        LOG(WARNING, "fail to make temp dir in merging pile: %s", temp_dir.c_str());
        return -1;
    }
    std::set<int> ready;
    int pile_num = pile_list.size();
    // Loop until all piles are ready. After every loop, sleep for 5s
    while (ready.size() < static_cast<size_t>(pile_num) && (sleep(5) || true)) {
        // Find a unproceeded pile
        int cur = CheckPileExecution(ready, pile_list);
        // For greater number of minions, just relax and wait for ready piles
        if (no_ >= pile_num) {
            continue;
        }
        int from = cur * pile_scale_;
        int to = std::min((cur + 1) * pile_scale_ - 1, total_ - 1);
        LOG(INFO, "merge from %d to %d as a pile", from, to);
        // Prepare specific range of sorted files for pre-merging
        std::vector<std::string> files;
        if (!PrepareSortFiles(files, from, to)) {
            continue;
        }
        std::stringstream ss;
        ss << temp_dir << "/" << no_ << ".pile";
        const std::string& output = ss.str();
        // Pre-merge
        if (!PreMerge(files, output)) {
            continue;
        }
        // Move pile to let others know
        ss.str(std::string(""));
        ss << work_dir_ << cur << ".pile";
        if (!fp_->Rename(output, ss.str())) {
            continue;
        }
        ready.insert(cur);
        LOG(INFO, "got pile, %d/%d is ready", ready.size(), pile_num);
    }
    // Remove temp dir
    fp_->Remove(temp_dir);
    return ready.size();
}

int ShuffleInlet::CheckPileExecution(std::set<int>& ready,
        const std::vector<int>& pile_list) {
    int cur = 0;
    for (std::vector<int>::const_iterator it = pile_list.begin();
            it != pile_list.end(); ++it) {
        cur = *it;
        if (ready.find(cur) != ready.end()) {
            // This pile has been proceeded
            continue;
        }
        std::stringstream ss;
        ss << work_dir_ << cur << ".pile";
        const std::string& pile_name = ss.str();
        if (!fp_->Exist(pile_name)) {
            break;
        }
        ready.insert(cur);
        LOG(INFO, "lucky, got %d/%d ready pile", ready.size(), pile_list.size());
    }
    return cur;
}

bool ShuffleInlet::PrepareSortFiles(std::vector<std::string>& files, int from, int to) {
    for (int i = from; i <= to; ++i) {
        std::stringstream ss;
        ss << work_dir_ << "node_" << phase_ << "_" << i;
        std::vector<FileInfo> sortfiles;
        if (!fp_->List(ss.str(), &sortfiles)) {
            LOG(WARNING, "fail to list: %s", ss.str().c_str());
            return false;
        }
        LOG(DEBUG, "list: %s, size: %d", ss.str().c_str(), sortfiles.size());
        for (std::vector<FileInfo>::iterator it = sortfiles.begin();
                it != sortfiles.end(); ++it) {
            const std::string& file_name = it->name;
            if (boost::ends_with(file_name, ".sort")) {
                files.push_back(file_name);
            }
        }
    }
    return true;
}

}
}

