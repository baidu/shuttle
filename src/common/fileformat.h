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
    virtual int64_t GetSize() = 0;

    // Formatted file related tools
    /*
     * Build a record from k/v
     *   plain text: if key is not empty, then key is considered as offset or number, record:
     *               [no/offset]\t[line]\n
     *               if key is empty, then the record is line iteself: [line]\n
     *   inf seqfile: build a binary record:
     *                [key_len(32bits)][key][value_len(32bits)][value]
     *   sort file: [NOT USED] build a binary record using protobuf:
     *              serialize({ key, value })
     */
    static std::string BuildRecord(FileFormat format,
            const std::string& key, const std::string& value);

    /*
     * Parse k/v from a record
     *   plain text: set value to the record, and leave key untouched
     *   inf seqfile: consider that record is formatted as following:
     *                [key_len(32bits)][key][value_len(32bits)][value]
     *                and get k/v from the proper area
     *   sort file: [NOT USED] get k/v from a serialized protobuf returned from BuildRecord
     */
    static bool ParseRecord(FileFormat format, const std::string& record,
            std::string& key, std::string& value);

    virtual ~FormattedFile() { }
};

}
}

#endif

