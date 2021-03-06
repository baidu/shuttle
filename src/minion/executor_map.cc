#include "executor.h"
#include <algorithm>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sstream>
#include <vector>
#include <logging.h>
#include "sort/sort_file.h"
#include "partition.h"

using baidu::common::WARNING;
using baidu::common::INFO;

namespace baidu {
namespace shuttle {

const static size_t sMaxInMemTable = 512 << 20;
const static size_t sMaxRecordSize = 2 << 20;

struct EmitItem {
    int reduce_no;
    std::string key;
    std::string record;
    EmitItem(int l_reduce_no, const std::string& l_key, const std::string& l_record) {
        reduce_no = l_reduce_no;
        key = l_key;
        record = l_record;
    }
    inline size_t Size() {
        return sizeof(int) + key.capacity() + record.capacity() + sizeof(EmitItem*);
    }
};

struct EmitItemLess {
    bool operator()(EmitItem* const& a , EmitItem* const& b) {
        if (a->reduce_no < b->reduce_no) {
            return true;
        } else if (a->reduce_no == b->reduce_no) {
            return a->key < b->key;
        } else {
            return false;
        }
    }
};

class Emitter {
public:
    Emitter(const std::string& work_dir, const TaskInfo& task) : task_(task) {
        work_dir_ = work_dir;
        cur_byte_size_ = 0;
        file_no_ = 0;
    }
    ~Emitter();
    Status Emit(int reduce_no, const std::string& key, const std::string& record) ;
    void Reset();
    Status FlushMemTable();
private:
    std::string work_dir_;
    size_t cur_byte_size_;
    std::vector<EmitItem*> mem_table_;
    int file_no_;
    const TaskInfo& task_;
};

MapExecutor::MapExecutor() {
    ::setenv("mapred_task_is_map", "true", 1);
}

MapExecutor::~MapExecutor() {

}

TaskState MapExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map task");
    ::setenv("mapred_work_output_dir", GetMapWorkDir(task).c_str(), 1);
    std::string cmd = "sh ./app_wrapper.sh \"" + task.job().map_command() + "\"";
    LOG(INFO, "map command is: %s", cmd.c_str());
    FILE* user_app = popen(cmd.c_str(), "r");
    if (user_app == NULL) {
        LOG(WARNING, "start user app fail, cmd is %s, (%s)", 
            cmd.c_str(), strerror(errno));
        return kTaskFailed;
    }

    KeyFieldBasedPartitioner key_field_partition(task);
    IntHashPartitioner int_hash_partition(task);
    Partitioner* partitioner = &key_field_partition;
    if (task.job().partition() == kIntHashPartitioner) {
        partitioner =  &int_hash_partition;
    }

    FileSystem::Param param;
    FillParam(param, task);
    FileSystem* fs = FileSystem::CreateInfHdfs(param);
    fs->Mkdirs(GetShuffleWorkDir(task));
    delete fs;

    Emitter emitter(GetMapWorkDir(task), task);
    if (task.job().pipe_style() == kStreaming) {
        TaskState state = StreamingShuffle(user_app, task, partitioner, &emitter);
        if (state != kTaskCompleted) {
            return state;
        }
    } else if (task.job().pipe_style() == kBiStreaming) {
        TaskState state = BiStreamingShuffle(user_app, task, partitioner, &emitter);
        if (state != kTaskCompleted) {
            return state;
        }
    } else {
        LOG(FATAL, "unkown output format: %d", task.job().output_format());
    }

    Status status = emitter.FlushMemTable();
    if (status != kOk) {
        LOG(WARNING, "flush fail, %s", Status_Name(status).c_str());
        return kTaskFailed;
    }
    int ret = pclose(user_app);
    if (ret != 0) {
        LOG(WARNING, "user app fail, cmd is %s, ret: %d", cmd.c_str(), ret);
        return kTaskFailed;
    }
    if (!MoveTempToShuffle(task)) {
        LOG(WARNING, "move map result to shuffle dir fail");
        return kTaskFailed;
    }
    return kTaskCompleted;
}

Emitter::~Emitter() {
    Reset();
}

void Emitter::Reset() {
    cur_byte_size_ = 0;
    std::vector<EmitItem*>::iterator it;
    for (it = mem_table_.begin(); it != mem_table_.end(); it++) {
        delete (*it);
    }
    std::vector<EmitItem*>().swap(mem_table_);
}

Status Emitter::Emit(int reduce_no, const std::string& key, const std::string& record) {
    EmitItem* item = new EmitItem(reduce_no, key, record);
    if (item->Size() > sMaxRecordSize) {
        LOG(WARNING, "ignore too large records");
        delete item;
        return kOk;
    }
    mem_table_.push_back(item);
    cur_byte_size_ += item->Size();
    
    if (cur_byte_size_ < sMaxInMemTable) {
        return kOk; //memtable is not big enough
    }

    return FlushMemTable();
}

Status Emitter::FlushMemTable() {
    SortFileWriter* writer = NULL;
    Status status = kOk;
    char file_name[4096];
    char s_reduce_no[256];
    do {
        std::sort(mem_table_.begin(), mem_table_.end(), EmitItemLess());
        writer = SortFileWriter::Create(kHdfsFile, &status);
        if (status != kOk) {
            break;
        }
        FileSystem::Param param;
        Executor::FillParam(param, task_);
        param["replica"] = "3";
        snprintf(file_name, sizeof(file_name), "%s/%d.sort",
                 work_dir_.c_str(), file_no_);
        status = writer->Open(file_name, param);
        if (status != kOk) {
            break;
        }
        if (mem_table_.empty()) {
            break;
        }

        std::vector<EmitItem*>::iterator it;
        for (it = mem_table_.begin(); it != mem_table_.end(); it++) {
            EmitItem* item = *it;
            snprintf(s_reduce_no, sizeof(s_reduce_no), "%05d", item->reduce_no);
            std::string raw_key = s_reduce_no;
            raw_key += "\t";
            raw_key += item->key;
            status = writer->Put(raw_key, item->record);
            if (status != kOk) {
                break;
            }
        }
    } while(0);
    
    if (status == kOk) {
        status = writer->Close();
        file_no_ ++;
    }
    delete writer;
    Reset();
    return status;
}


TaskState MapExecutor::StreamingShuffle(FILE* user_app, const TaskInfo& task,
                                        const Partitioner* partitioner, Emitter* emitter) {
    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            pclose(user_app);
            return kTaskCanceled;
        }
        if (fgets(line_buf_, sLineBufferSize, user_app) == NULL) {
            break;
        }
        std::string record(line_buf_);
        std::string key;
        int reduce_no;
        if (record.size() > 0 && record[record.size() - 1] == '\n') {
            record.erase(record.size() - 1);
        }
        if (record.empty()) {
            continue;
        }
        reduce_no = partitioner->Calc(record, &key);
        Status em_status = emitter->Emit(reduce_no, key, record);
        if (em_status != kOk) {
            LOG(WARNING, "emit fail, %s, %s", record.c_str(),
                Status_Name(em_status).c_str());
            return kTaskFailed;
        }
    }
    return kTaskCompleted;
}

TaskState MapExecutor::BiStreamingShuffle(FILE* user_app, const TaskInfo& task,
                                          const Partitioner* partitioner, Emitter* emitter) {
    while (!feof(user_app)) {
        if (ShouldStop(task.task_id())) {
            LOG(WARNING, "task: %d is canceled.", task.task_id());
            pclose(user_app);
            return kTaskCanceled;
        }
        std::string key;
        std::string value;
        if (!ReadRecord(user_app, &key, &value)) {
            LOG(WARNING, "read user app fail");
            return kTaskFailed;
        }
        if (feof(user_app)) {
            break;
        }
        std::string sort_key;
        int reduce_no = partitioner->Calc(key, &sort_key);
        std::string record;
        int32_t key_len = key.size();
        int32_t value_len = value.size();
        record.append((const char*)(&key_len), sizeof(key_len));
        record.append(key);
        record.append((const char*)(&value_len), sizeof(value_len));
        record.append(value);
        Status em_status = emitter->Emit(reduce_no, sort_key, record);
        if (em_status != kOk) {
            LOG(WARNING, "emit fail, %s, %s", record.c_str(),
                Status_Name(em_status).c_str());
            return kTaskFailed;
        }
    }
    return kTaskCompleted;
}

}
}
