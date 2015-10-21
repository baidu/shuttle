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

const static int32_t sLineBufSize = 40960;
const static size_t sMaxInMemTable = 512 << 20;

struct EmitItem {
    int reduce_no;
    std::string key;
    std::string record;
    EmitItem(int l_reduce_no, const std::string& l_key, const std::string& l_record) {
        reduce_no = l_reduce_no;
        key = l_key;
        record = l_record;
    }
    size_t Size() {
        return sizeof(int) + key.size() + record.size();
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

MapExecutor::MapExecutor() : line_buf_(NULL) {
    ::setenv("mapred_task_is_map", "true", 1);
    line_buf_ = (char*)malloc(sLineBufSize);
}

MapExecutor::~MapExecutor() {
    free(line_buf_);
}

TaskState MapExecutor::Exec(const TaskInfo& task) {
    LOG(INFO, "exec map task");
    std::string cmd = "sh ./app_wrapper.sh " + task.job().map_command();
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
        return kTaskMoveOutputFailed;
    }
    return kTaskCompleted;
}

void Emitter::Reset() {
    cur_byte_size_ = 0;
    std::vector<EmitItem*>::iterator it;
    for (it = mem_table_.begin(); it != mem_table_.end(); it++) {
        delete (*it);
    }
    mem_table_.clear();   
}

Status Emitter::Emit(int reduce_no, const std::string& key, const std::string& record) {
    EmitItem* item = new EmitItem(reduce_no, key, record);
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
        param["replica"] = "2";
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
            return kTaskCanceled;
        }
        if (fgets(line_buf_, sLineBufSize, user_app) == NULL) {
            break;
        }
        std::string record(line_buf_);
        std::string key;
        int reduce_no;
        if (record.size() > 0 && record[record.size() - 1] == '\n') {
            record.erase(record.size() - 1);
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
        int reduce_no = partitioner->Calc(key);
        std::string record;
        int32_t key_len = key.size();
        int32_t value_len = value.size();
        record.append((const char*)(&key_len), sizeof(key_len));
        record.append(key);
        record.append((const char*)(&value_len), sizeof(value_len));
        record.append(value);
        Status em_status = emitter->Emit(reduce_no, key, record);
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
