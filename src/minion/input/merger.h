#ifndef _BAIDU_SHUTTLE_MERGER_H_
#define _BAIDU_SHUTTLE_MERGER_H_
#include "common/io_type.h"
#include "common/fileformat.h"
#include <queue>
#include <vector>

namespace baidu {
namespace shuttle {

class Merger : public KVScanner {
public:
    Merger();
    virtual ~Merger();

    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual Status Open(const std::string& path, const File::Param& param);
    virtual Status Close();
    virtual std::string GetFileName();

    static const int PARALLEL_LEVEL = 12;

    class MergeItem {
    public:
        std::string key;
        std::string value;
        int file_offset;
        MergeItem(const std::string& key, const std::string& value, int offset) :
            key(key), value(value), file_offset(offset) { }
        bool operator<(const MergeItem& other) const {
            return key > other.key;
        }
    };

    class Iterator : public Iterator {
    public:
        Iterator(const std::vector<FormattedFile*>& files, const std::string& end_key);
        virtual ~Iterator();
        virtual bool Done();
        virtual void Next();
        virtual Status Error() {
            return status_;
        }
        virtual const std::string& Key() {
            return key_;
        }
        virtual const std::string& Value() {
            return value_;
        }
        virtual std::string GetFileName();
    private:
        std::string end_key_;
        std::string key_;
        std::string value_;
        Status status_;
        std::priority_queue<MergeItem> queue_;
        std::vector<FormattedFile*> sortfiles_;
    };

private:
    void AddProvedFile(const std::string& start_key, FormattedFile* fp,
            std::vector<FormattedFile*>& to_be_scanned, Mutex& vec_mu);

private:
    std::vector<FormattedFile*> sortfiles_;
};

}
}

#endif

