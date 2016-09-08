#include "inlet.h"

#include "minion/input/merger.h"
#include "common/fileformat.h"
#include "common/scanner.h"
#include <iostream>

namespace baidu {
namespace shuttle {

int SourceInlet::Flow() {
    // Prepare scanner
    FileType type = kInfHdfs;
    FileFormat format = kPlainText;
    if (type_ == "hdfs") {
        type = kInfHdfs;
    } else if (type_ == "local") {
        type = kLocalFs;
    } else {
        std::cerr << "unknown file system: " << type_ << std::endl;
        return -1;
    }
    if (format_ == "text") {
        format = kPlainText;
    } else if (format == "seq") {
        format = kInfSeqFile;
    } else {
        std::cerr << "unknown file format: " << format_ << std::endl;
        return -1;
    }
    File::Param param;
    // TODO Fill param
    FormattedFile* fp = FormattedFile::Create(type, format, param);
    if (!fp->Open(file_, kReadFile, param)) {
        std::cerr << "fail to open: " << file_ << std::endl;
        return 1;
    }
    Scanner* scanner = Scanner::Get(fp, kInputScanner);
    Scanner::Iterator* it = scanner->Scan();

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
        std::cerr << "error occurs when reading: " << file_ << std::endl;
        return -1;
    }

    delete it;
    delete scanner;
    fp->Close();
    delete fp;
    return 0;
}

int ShuffleInlet::Flow() {
    // TODO
    return -1;
}

// Use do...while(0) to achieve 'finally' function
bool ShuffleInlet::PreMerge(const std::vector<std::string>& files,
        const std::string& output) {
    File::Param param;
    // TODO Fill param
    FormattedFile* writer = FormattedFile::Create(kInfHdfs, kInternalSortedFile, param);
    if (writer == NULL) {
        std::cerr << "empty output file pointer: " << output << std::endl;
        return false;
    }
    if (!writer->Open(output, kWriteFile, param)) {
        std::cerr << "fail to open output file: " << output << std::endl;
        return false;
    }
    bool ok = true;
    // Writer is opened
    do {
        std::vector<FormattedFile*> merge_files;
        for (std::vector<std::string>::iterator it = files.begin();
                it != files.end(); ++it) {
            FormattedFile* fp = FormattedFile::Create(kInfHdfs, kInternalSortedFile, param);
            if (fp == NULL) {
                continue;
            }
            if (!fp->Open(*it, kReadFile, param)) {
                std::cerr << "Fail to open: " << *it << std::endl;
                delete fp;
                continue;
            }
            merge_files.push_back(fp);
        }
        if (merge_files.empty()) {
            std::cerr << "no valid pre-merge file" << std::endl;
            ok = false;
            break;
        }
        // Input files are now opened
        do {
            Merger merger(merge_files);
            Scanner::Iterator* it = merger.Scan(SCAN_KEY_BEGINNING, SCAN_ALL_KEY);
            if (it == NULL) {
                std::cerr << "fail to get scan iterator" << std::endl;
                ok = false;
                break;
            }
            if (it->Error() != kOk && it->Error() != kNoMore) {
                std::cerr << "fail to scan, error file: " << it->GetFileName() << std::endl;
                ok = false;
                break;
            }
            for (; !it->Done(); it->Next()) {
                if (!writer->WriteRecord(it->Key(), it->Value())) {
                    std::cerr << "fail to write (" << it->Key() << ", " << it->Value()
                        << "): " << output << std::endl;
                    ok = false;
                    break;
                }
            }
            if (it->Error() != kOk && it->Error() != kNoMore) {
                std::cerr << "fail to scan: " << it->GetFileName() << std::endl;
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
    File::Param param;
    // TODO Fill param
    std::vector<FormattedFile*> merge_files;
    for (std::vector<std::string>::iterator it = files.begin();
            it != files.end(); ++it) {
        FormattedFile* fp = FormattedFile::Create(kInfHdfs, kInternalSortedFile, param);
        if (fp == NULL) {
            continue;
        }
        if (!fp->Open(*it, kReadFile, param)) {
            std::cerr << "Fail to open: " << *it << std::endl;
            delete fp;
            continue;
        }
        merge_files.push_back(fp);
    }
    if (merge_files.empty()) {
        std::cerr << "no valid pre-merge file" << std::endl;
        return false;
    }
    bool ok = true;
    do {
        std::stringstream ss;
        ss << std::setw(5) << std::setfill('0') << no_;
        Merger merger(merge_files);
        // Scan all the records of the same no
        Scanner::Iterator* it = merger.Scan(ss.str(), ss.str() + "\xff");
        if (it == NULL) {
            std::cerr << "fail to get scan iterator" << std::endl;
            ok = false;
            break;
        }
        if (it->Error() != kOk && it->Error() != kNoMore) {
            std::cerr << "fail to scan, error file: " << it->GetFileName() << std::endl;
            ok = false;
            break;
        }
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
            std::cerr << "fail to scan: " << it->GetFileName() << std::endl;
            ok = false;
        }
    } while(0);
    for (std::vector<FormattedFile*>::iterator it = merge_files.begin();
            it != merge_files.end(); ++it) {
        (*it)->Close();
        delete (*it);
    }
    return ok;
}

}
}

