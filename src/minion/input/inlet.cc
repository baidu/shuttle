#include "inlet.h"

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

    int no = 0;
    // TODO More consideration
    for (; !it->Done(); it->Next()) {
        std::string record;
        if (fp->BuildRecord(it->key, it->value, record)) {
            continue;
        }
        if (is_nline_) {
            std::cout << no << "\t" << record;
            ++no;
        } else {
            std::cout << record;
        }
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

}
}

