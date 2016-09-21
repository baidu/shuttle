#include "outlet.h"

#include "minion/output/partition.h"
#include "minion/output/hopper.h"
#include "common/file.h"
#include <sstream>
#include <iomanip>
#include "logging.h"

namespace baidu {
namespace shuttle {

FormattedFile* Outlet::GetFileWrapper(FILE* fp, const std::string& pipe) {
    File* inner = File::Get(kLocalFs, fp);
    if (inner == NULL) {
        LOG(WARNING, "fail to wrap stdin, die");
        return NULL;
    }
    FormattedFile* f = NULL;
    if (pipe == "streaming") {
        f = new TextStream(inner);
    } else if (pipe == "bistreaming") {
        f = new BinaryStream(inner);
    }
    if (f == NULL) {
        LOG(WARNING, "fail to get formatted file and parse input of pipe %s",
                pipe.c_str());
        return NULL;
    }
    return f;
}

int InternalOutlet::Collect() {
    Partitioner* partitioner = GetPartitioner();
    FormattedFile* fp = GetFileWrapper(stdin, pipe_);
    Hopper hopper(work_dir_, type_, param_);
    bool textstream = pipe_ == "streaming";
    std::string key, value;
    while (fp->ReadRecord(key, value)) {
        HopperItem item;
        const std::string& raw_key = textstream ? value : key;
        item.dest = partitioner->Calc(raw_key, &item.key);
        item.record = fp->BuildRecord(item.key, value);
        Status status = hopper.Emit(&item);
        if (status != kOk) {
            LOG(WARNING, "fail to emit `%s' to output: %s",
                    item.record.c_str(), Status_Name(status).c_str());
            return 1;
        }
    }
    if (fp->Error() != kOk && fp->Error() != kNoMore) {
        LOG(WARNING, "read record stops due to %s", Status_Name(fp->Error()).c_str());
    }
    Status status = hopper.Flush();
    if (status != kOk && status != kNoMore) {
        LOG(WARNING, "fail to flush data to output: %s", Status_Name(status).c_str());
    }
    delete fp;
    delete partitioner;
    return 0;
}

Partitioner* InternalOutlet::GetPartitioner() {
    Partition p = kKeyFieldBasedPartitioner;
    if (partition_ == "keyhash") {
        p = kKeyFieldBasedPartitioner;
    } else if (partition_ == "inthash") {
        p = kIntHashPartitioner;
    } else {
        LOG(WARNING, "unfamiliar partitioner type: %s", partition_.c_str());
        return NULL;
    }
    Partitioner* pt = Partitioner::Get(p, separator_,
            key_fields_, partition_fields_, dest_num_);
    if (pt == NULL) {
        LOG(WARNING, "fail to get partitioner to parse key");
        return NULL;
    }
    return pt;
}

int ResultOutlet::Collect() {
    FormattedFile* fin = GetFileWrapper(stdin, pipe_);
    if (fin == NULL) {
        return 1;
    }
    std::stringstream output_ss;
    output_ss << work_dir_ << std::setw(5) << std::setfill('0') << no_;
    filename_ = output_ss.str();
    if (!PrepareOutputFiles()) {
        delete fin;
        return 1;
    }
    textoutput_ = format_ == "text";

    do {
        std::string key, value;
        while (fin->ReadRecord(key, value)) {
            if (!WriteToOutput(key, value)) {
                break;
            }
        }
        if (fin->Error() != kOk && fin->Error() != kNoMore) {
            LOG(WARNING, "read record stops due to %s", Status_Name(fin->Error()).c_str());
        }
    } while (0);

    delete fin;
    return 0;
}

bool ResultOutlet::PrepareOutputFiles() {
    if (format_ != "multiple") {
        output_pool_.resize(1, NULL);
        if (format_ == "text") {
            fileformat_ = kPlainText;
        } else if (format_ == "seq") {
            fileformat_ = kInfSeqFile;
        } else {
            LOG(WARNING, "unknown file format: %s", format_.c_str());
            return false;
        }
        FormattedFile* fp = FormattedFile::Create(type_, fileformat_, param_);
        if (fp == NULL) {
            LOG(WARNING, "fail to get file pointer");
            return false;
        }
        if (!fp->Open(filename_, kWriteFile, param_)) {
            LOG(WARNING, "fail to open output file: %s", filename_.c_str());
            delete fp;
            return false;
        }
        output_pool_[0] = fp;
        multiplex_ = false;
    } else {
        output_pool_.resize(26, NULL);
        multiplex_ = true;
    }
    return true;
}

bool ResultOutlet::WriteToOutput(const std::string& key, const std::string& value) {
    FormattedFile* fp = NULL;
    std::string result_val(value);
    if (multiplex_) {
        int offset = 0;
        /*
         * Value should at least contain separator and multiplex info
         * Otherwise use the first file to output
         */
        if (value.size() >= 2) {
            char suffix = *value.rbegin();
            if (suffix >= 'A' && suffix <= 'Z') {
                offset = suffix - 'A';
                result_val.erase(result_val.end() - 2, result_val.end());
            }
        }
        fp = GetOutputFile(offset);
    } else {
        fp = GetOutputFile(0);
    }
    if (fp == NULL) {
        LOG(WARNING, "fail to write data to nullptr");
        return false;
    }
    if (!textoutput_) {
        result_val = key + "\t" + result_val;
    }
    if (!fp->WriteRecord(key, result_val)) {
        LOG(WARNING, "fail to write record");
        return false;
    }
    return true;
}

FormattedFile* ResultOutlet::GetOutputFile(int no) {
    if (static_cast<size_t>(no) > output_pool_.size()) {
        return NULL;
    }
    if (output_pool_[no] != NULL) {
        return output_pool_[no];
    }
    FormattedFile* fp = FormattedFile::Create(type_, fileformat_, param_);
    if (fp == NULL) {
        LOG(WARNING, "fail to get file pointer");
        return false;
    }
    char suffix = 'A' + no;
    if (!fp->Open(filename_ + "_" + suffix, kWriteFile, param_)) {
        LOG(WARNING, "fail to open output file: %s", filename_.c_str());
        delete fp;
        return NULL;
    }
    output_pool_[no] = fp;
    return fp;
}

}
}

