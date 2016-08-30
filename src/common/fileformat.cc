#include "fileformat.h"

#include "file.h"
#include "hdfs.h"

namespace baidu {
namespace shuttle {

namespace factory {

FormattedFile* GetPlainTextFile(File* fp);
FormattedFile* GetSeqFile(hdfsFS fs);
FormattedFile* GetSortFile(File* fp);

}

FormattedFile* FormattedFile::Get(File* fp, FileFormat format) {
    switch(format) {
    case kPlainText:
        return factory::GetPlainTextFile(fp);
    case kInfSeqFile:
        // SeqFile needs to be with HDFS file, so not work on general file pointer
        return NULL;
    case kInternalSortedFile:
        return factory::GetSortFile(fp);
    }
    return NULL;
}

FormattedFile* FormattedFile::Create(FileType type, FileFormat format,
        const File::Param& param) {
    switch(format) {
    case kPlainText:
    case kInternalSortedFile:
        File* fp = File::Create(type, param);
        return Get(fp, format);
    case kInfSeqFile:
        if (type != kInfHdfs) {
            return NULL;
        }
        hdfsFS fs = NULL;
        if (File::ConnectInfHdfs(param, &fs)) {
            return factory::GetSeqFile(fs);
        }
    }
    return NULL;
}

}
}

