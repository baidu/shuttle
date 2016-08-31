#ifndef _BAIDU_SHUTTLE_MERGER_H_
#define _BAIDU_SHUTTLE_MERGER_H_
#include "common/scanner.h"
#include "common/fileformat.h"
#include <queue>
#include <vector>
#include "mutex.h"

namespace baidu {
namespace shuttle {

class Merger : public Scanner {
public:
    // Merger requires opened files
    Merger(const std::vector<FormattedFile*> files);
    // Merger is not the owner of files. Caller must delete them by himself
    virtual ~Merger() { }

    virtual Iterator* Scan(const std::string& start_key, const std::string& end_key);
    virtual std::string GetFileName() {
        // Not Implement, inherited from scanner interface
        return "";
    }

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

    class Iterator : public Scanner::Iterator {
    public:
        Iterator(const std::vector<Scanner::Iterator*>& iters);
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
        // Use this interface to return err file
        virtual std::string GetFileName() {
            return GetErrorFile();
        }

        std::string GetErrorFile() {
            return err_file_;
        }
    private:
        std::string key_;
        std::string value_;
        Status status_;
        std::string err_file_;
        std::priority_queue<MergeItem> queue_;
        std::vector<Scanner::Iterator*> iters_;
    };

private:
    void AddProvedIter(const std::string& start_key, const std::string& end_key,
            FormattedFile* fp, std::vector<Scanner::Iterator*>& to_be_scanned, Mutex* vec_mu);

private:
    std::vector<FormattedFile*> sortfiles_;
};

}
}

#endif

