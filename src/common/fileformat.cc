#include "fileformat.h"

#include "file.h"
#include "hdfs.h"
#include "proto/sortfile.pb.h"

namespace baidu {
namespace shuttle {

namespace factory {

FormattedFile* GetPlainTextFile(File* fp);
FormattedFile* GetInfSeqFile(hdfsFS fs);
FormattedFile* GetSortFile(File* fp);

}

FormattedFile* FormattedFile::Get(File* fp, FileFormat format) {
    if (fp == NULL) {
        return NULL;
    }
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
            return factory::GetInfSeqFile(fs);
        }
    }
    return NULL;
}

std::string FormattedFile::BuildRecord(FileFormat format,
        const std::string& key, const std::string& value) {
    std::string record;
    switch(format) {
    case kPlainText:
        if (!key.empty()) {
            record += key;
            record.push_back('\t');
        }
        record += value;
        if (*record.rbegin() != '\n') {
            record.push_back('\n');
        }
        break;
    case kInfSeqFile:
        uint32_t key_len = key.size();
        uint32_t value_len = value.size();
        record.append((const char*)&key_len, sizeof(key_len));
        record.append(key);
        record.append((const char*)&value_len, sizeof(value_len));
        record.append(value);
        break;
    case kInternalSortedFile:
        KeyValue rec;
        rec.set_key(key);
        rec.set_value(value);
        rec.SerializeToString(&record);
        break;
    // leave record empty by default
    }
    return record;
}

}
}

