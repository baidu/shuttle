#ifndef _BAIDU_SHUTTLE_SORT_FILE_H_
#define _BAIDU_SHUTTLE_SORT_FILE_H_
#include "common/fileformat.h"
#include "proto/sortfile.pb.h"

namespace baidu {
namespace shuttle {

/*
 * Sortfile is an internal compressed binary file format, the file format is as following
 * +---------------------------------------+
 * | Block size |  Data Block(compressed)  |
 * +---------------------------------------+
 * |                  ...                  |
 * +---------------------------------------+
 * | Block size | Index Block(compressed)  |
 * +---------------------------------------+
 * |   Index offset   |    Magic number    |
 * +---------------------------------------+
 * Index block is composed by first entries of every data block, as content of block
 * Magic number is to check the completion of a file
 */

class SortFile : public FormattedFile {
public:
    SortFile(File* fp) : fp_(fp), status_(kOk), cur_block_offset_(UINT64_MAX) { }
    virtual ~SortFile() {
        delete fp_;
    }

    // Before reading a record, Locate() must be called first
    virtual bool ReadRecord(std::string& key, std::string& value);
    /*
     * WriteRecord needs that caller provides sorted KV data, this is only
     *   the file format and do not handle sorting
     */
    virtual bool WriteRecord(const std::string& key, const std::string& value);
    /*
     * Since index block may be sparse so the function will iterate over file to locate
     *   the key and might be a little bit long when data is remotely read via network
     */
    virtual bool Locate(const std::string& key);
    virtual bool Seek(int64_t /*offset*/) {
        // TODO not implement, not qualified to be input file
        return false;
    }
    virtual int64_t Tell() {
        // TODO not implement, not qualified to be input file
        return -1;
    }

    virtual bool Open(const std::string& path, OpenMode mode, const File::Param& param);
    virtual bool Close();

    virtual Status Error() {
        return status_;
    }

    virtual std::string GetFileName() {
        return path_;
    }
    virtual int64_t GetSize();

    virtual std::string BuildRecord(const std::string& key, const std::string& value) {
        return FormattedFile::BuildRecord(kInternalSortedFile, key, value);
    }

    static const int64_t BLOCK_SIZE = (64 << 10);
    static const int32_t MAX_INDEX_SIZE = 10000;
    static const int32_t MAGIC_NUMBER = 0x55aa;
protected:
    // ----- Methods for reading -----
    bool LoadIndexBlock(IndexBlock& index);
    bool LoadDataBlock(DataBlock& block);

    // ----- Methods for writing -----
    bool FlushCurBlock();
    bool FlushIdxBlock();
    bool WriteSerializedBlock(const std::string& block);
    // Index block must fit in memory. This method is to keep index block in proper size
    bool MakeIndexSparse();

protected:
    // Non-Nullpointer ensured
    File* fp_;
    OpenMode mode_;
    std::string path_;
    Status status_;

    // ----- Members for reading -----
    DataBlock cur_block_;
    int64_t cur_block_offset_;
    int64_t idx_offset_;

    // ----- Members for writing -----
    std::string last_key_;
    IndexBlock idx_block_;
    // DataBlock cur_block_;
    // int64_t cur_block_offset_;
};

}
}

#endif

