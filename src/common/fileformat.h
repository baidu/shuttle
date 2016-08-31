#ifndef _BAIDU_SHUTTLE_FILEFORMAT_H_
#define _BAIDU_SHUTTLE_FILEFORMAT_H_
#include <stdint.h>
#include "file.h"
#include "proto/shuttle.pb.h"

namespace baidu {
namespace shuttle {

enum FileFormat {
    // Qualified to be input files, not qualified to be internal file
    kPlainText = 1,
    // Qualified to be input files, not qualified to be internal file
    kInfSeqFile = 2,
    // Qualified to be internal files, not qualified to be input file
    kInternalSortedFile = 3
};

class FormattedFile {
public:
    /*
     * 1. Ownership of file pointer is changed to FormattedFile, so it will be deleted automatically
     * 2. Since InfSeqFile has to be on HDFS, this interface returns NULL when format == kInfSeqFile
     */
    static FormattedFile* Get(File* fp, FileFormat format);
    static FormattedFile* Create(FileType type, FileFormat format, const File::Param& param);
    /*
     * Record here is automatically aligned, which means if specified offset is in the middle
     *   of a record, ReadRecord will ignore the rest of current record and
     *   get next complete record
     */
    /*
     * Records in plain text is simply text lines, so key is not used in following two interfaces,
     *   key will be set to empty while reading and will be ignored while writing
     */
    virtual bool ReadRecord(std::string& key, std::string& value) = 0;
    virtual bool WriteRecord(const std::string& key, const std::string& value) = 0;
    /*
     * A formatted file implemented this interface is qualified to be the internal middle files,
     *   since merging needs to get a record location by key
     */
    virtual bool Locate(const std::string& key) = 0;
    /*
     * A formatted file implemented following two interfaces is qualified to be the input file,
     *   since input files will be divided by size in master's resource manager
     */
    virtual bool Seek(int64_t offset) = 0;
    virtual int64_t Tell() = 0;

    virtual bool Open(const std::string& path, OpenMode mode, const File::Param& param) = 0;
    virtual bool Close() = 0;

    /*
     * Get current status, influenced by lastest operation
     */
    virtual Status Error() = 0;

    virtual std::string GetFileName() = 0;

    /*
     * Parsing a record, which is currently not used
     */
    // virtual bool ParseRecord(const std::string& record, std::string& key, std::string& value) = 0;
    /*
     * Build a record from k/v
     */
    virtual bool BuildRecord(const std::string& key, const std::string& value,
            std::string& record) = 0;

    virtual ~FormattedFile() { }
};

}
}

#endif

