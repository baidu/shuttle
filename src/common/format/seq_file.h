#ifndef _BAIDU_SHUTTLE_SEQ_FILE_H_
#define _BAIDU_SHUTTLE_SEQ_FILE_H_
#include "common/fileformat.h"
#include "hdfs.h"

namespace baidu {
namespace shuttle {

/*
 * Sequence file is a binary file format in libhdfs,
 *   which can be compressed to save storage.
 *   This class is a wrapper for libhdfs C functions
 */
class InfSeqFile : public FormattedFile {
public:
    InfSeqFile(hdfsFS fs) : fs_(fs), sf_(NULL), status_(kOk) { }
    virtual ~InfSeqFile() {
        Close();
        hdfsDisconnect(fs_);
    }

    virtual bool ReadRecord(std::string& key, std::string& value);
    virtual bool WriteRecord(const std::string& key, const std::string& value);
    virtual bool Locate(const std::string& /*key*/) {
        // TODO not implement, not qualified to be internal sorted file
        return false;
    }
    virtual bool Seek(int64_t offset);
    virtual int64_t Tell();

    virtual bool Open(const std::string& path, OpenMode mode, const File::Param& param);
    virtual bool Close();

    virtual Status Error() {
        return status_;
    }

    virtual std::string GetFileName() {
        return path_;
    }
    virtual int64_t GetSize();
private:
    hdfsFS fs_;
    SeqFile sf_;
    std::string path_;
    Status status_;
};


}
}

#endif

