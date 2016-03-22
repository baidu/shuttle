#include <sstream>
#include <iostream>
#include <gflags/gflags.h>
#include <map>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <sys/types.h>
#include <sys/wait.h>
#include "sort_file.h"
#include "minion/partition.h"
#include "logging.h"
#include "common/filesystem.h"
#include "common/tools_util.h"
#include "thread.h"
#include "mutex.h"

DEFINE_string(pipe, "streaming", "pipe style: streaming/bistreaming");
DEFINE_string(cmd, "cat", "user command be invoked");
DEFINE_bool(is_inthash, false, "use IntHasPartitioner or not");
DEFINE_int32(num_key_fields, 1, "number of key fileds");
DEFINE_string(separator, "\t", "sperator used to split line in to fileds");

const static size_t sMaxInMemTable = 256 << 20;
const static int sKeyLimit = 65536;

using baidu::common::Log;
using baidu::common::FATAL;
using baidu::common::INFO;
using baidu::common::WARNING;
using namespace baidu;
using namespace baidu::shuttle;

char g_line_buffer[409600];

bool ReadLine(FILE* input_file, std::string* line) {
    char * s = fgets(g_line_buffer, sizeof(g_line_buffer), input_file);
    if (s == NULL && feof(input_file)) {
        return true;
    }
    if (ferror(input_file)) {
        return false;
    }
    *line = g_line_buffer;
    return true;
}

bool ReadRecord(FILE* input_file, std::string* p_key, std::string* p_value) {
    int32_t key_len = 0;
    int32_t value_len = 0;
    std::string& key = *p_key;
    std::string& value = *p_value;
    if (fread(&key_len, sizeof(key_len), 1, input_file) != 1) {
        if (feof(input_file)) {
            return true;
        }
        LOG(WARNING, "read key_len fail");
        return false;
    }
    if (key_len < 0 || key_len > sKeyLimit) {
        LOG(WARNING, "invalid key len: %d", key_len);
        return false;
    }
    key.resize(key_len);
    if ((int32_t)fread(&key[0], sizeof(char), key_len, input_file) != key_len) {
        LOG(WARNING, "read key fail");
        return false;
    }
    if (fread(&value_len, sizeof(value_len), 1, input_file) != 1) {
        LOG(WARNING, "read value_len fail");
        return false;
    }
    value.resize(value_len);
    if ((int32_t)fread(&value[0], sizeof(char), value_len, input_file) != value_len) {
        LOG(WARNING, "read value fail");
        return false;
    }
    return true;
}

struct EmitItem {
    std::string key;
    std::string record;
    EmitItem(const std::string& l_key, const std::string& l_record) {
        key = l_key;
        record = l_record;
    }
    size_t Size() {
        return key.capacity() + record.capacity() + sizeof(EmitItem*);
    }
};

struct EmitItemLess {
    bool operator()(EmitItem* const& a , EmitItem* const& b) {
        return a->key < b->key;
    }
};

class Combiner {
public:
    Combiner(const std::string cmd) : cur_byte_size_(0) {
        user_cmd_ = cmd;
    }
    ~Combiner();
    Status Emit(const std::string& key, const std::string& record) ;
    void Reset();
    Status InvokeUserCombiner();
    void FlushSortedData(int child_pid, int out_fd);
private:
    size_t cur_byte_size_;
    std::vector<EmitItem*> mem_table_;
    std::string user_cmd_;
};

void Combiner::FlushSortedData(int /*child_pid*/, int out_fd) {
    std::sort(mem_table_.begin(), mem_table_.end(), EmitItemLess());
    std::vector<EmitItem*>::iterator it;
    FILE* out_file = fdopen(out_fd, "w");
    for (it = mem_table_.begin(); it != mem_table_.end(); it++) {
        EmitItem* item = *it;
        fwrite(item->record.data(), 1, item->record.size(), out_file);
    }
    fflush(out_file);
    fclose(out_file);
    close(out_fd);
}

Status Combiner::Emit(const std::string& key, const std::string& record) {
    EmitItem* item = new EmitItem(key, record);
    mem_table_.push_back(item);
    cur_byte_size_ += item->Size();
    if (cur_byte_size_ < sMaxInMemTable) {
        return kOk; //memtable is not big enough
    }
    return InvokeUserCombiner();
}

Combiner::~Combiner() {
    Reset();
}

void Combiner::Reset() {
    cur_byte_size_ = 0;
    std::vector<EmitItem*>::iterator it;
    for (it = mem_table_.begin(); it != mem_table_.end(); it++) {
        delete (*it);
    }
    std::vector<EmitItem*>().swap(mem_table_);
}

Status Combiner::InvokeUserCombiner () {
    int stdin_pipes[2];
    int stdout_pipes[2];
    pipe(stdin_pipes);
    pipe(stdout_pipes);
    pid_t child_pid = fork();
    LOG(INFO, "invoke combiner: %s", user_cmd_.c_str());
    if (child_pid == -1) {
        LOG(WARNING, "failed to fork child process");
        exit(1);
    } else if (child_pid == 0) { //child
        close(0);
        close(1);
        close(stdin_pipes[1]);
        close(stdout_pipes[0]);
        dup2(stdin_pipes[0], 0);
        dup2(stdout_pipes[1], 1);
        char* cmd_argv[] = {(char*)"sh", (char*)"-c", (char*)user_cmd_.c_str(), NULL};
        char* env[] = {NULL};
        ::execve("/bin/sh", cmd_argv, env);
        assert(0);//will not run here
    }
    //start another thread to write to child process's stdin
    close(stdin_pipes[0]);
    close(stdout_pipes[1]);
    common::Thread bg;
    bg.Start(boost::bind(&Combiner::FlushSortedData, this, child_pid, stdin_pipes[1]));
    char block_buffer[4096];
    FILE* child_stdout = fdopen(stdout_pipes[0], "r");
    while (!feof(child_stdout)) {
        int n_bytes = fread(block_buffer, 1, sizeof(block_buffer), child_stdout);
        if (n_bytes <= 0) {
            break;
        }
        if (fwrite(block_buffer, 1, n_bytes, stdout) != (size_t)n_bytes) {
            return kWriteFileFail;
        }
    }
    fflush(stdout);
    fclose(child_stdout);
    close(stdout_pipes[0]);
    int status;
    waitpid(child_pid, &status, 0);
    LOG(INFO, "child process exit with status: %d", status);
    bg.Join();
    Reset();//after 
    if (status != 0 ) {
        return kUnKnown;
    }
    return kOk;
}

int main(int argc, char* argv[]) {
    baidu::common::SetLogFile(GetLogName("./combine_tool.log").c_str());
    baidu::common::SetWarningFile(GetLogName("./combine_tool.log.wf").c_str());
    google::ParseCommandLineFlags(&argc, &argv, true);
    KeyFieldBasedPartitioner key_field_partition(FLAGS_num_key_fields, 1, 1, FLAGS_separator);
    IntHashPartitioner int_hash_partition(1, FLAGS_separator);
    Partitioner* partitioner = &key_field_partition;
    if (FLAGS_is_inthash) {
        partitioner =  &int_hash_partition;
    }
    Combiner combiner(FLAGS_cmd);
    if (FLAGS_pipe == "streaming") {
        std::string line;
        std::string key;
        bool read_ok = true;
        while (read_ok = ReadLine(stdin, &line) && !feof(stdin)) {
            partitioner->Calc(line, &key);
            if (combiner.Emit(key, line) != kOk) {
                LOG(WARNING, "fail to emit data to combiner");
                exit(1);
            }
        }
        if (combiner.InvokeUserCombiner() != kOk) {
            LOG(WARNING, "fail to invoke user combiner");
            exit(1);
        }
    } else if (FLAGS_pipe == "bistreaming") {
        std::string key;
        std::string sorted_key;
        std::string value;
        bool read_ok = true;
        while (read_ok = ReadRecord(stdin, &key, &value) && !feof(stdin)) {
            partitioner->Calc(key, &sorted_key);
            std::string record;
            int32_t key_len = key.size();
            int32_t value_len = value.size();
            record.append((const char*)(&key_len), sizeof(key_len));
            record.append(key);
            record.append((const char*)(&value_len), sizeof(value_len));
            record.append(value);
            if (combiner.Emit(sorted_key, record) != kOk) {
                LOG(WARNING, "fail to emit data to combiner");
                exit(1);
            }
        }
        if (combiner.InvokeUserCombiner() != kOk) {
            LOG(WARNING, "fail to invoke user combiner");
            exit(1);
        }
    } else {
        LOG(WARNING, "unkown pipe style: %s", FLAGS_pipe.c_str());
        return 1;
    }
    return 0;
}
