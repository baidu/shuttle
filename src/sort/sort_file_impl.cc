#include "sort_file_hdfs.h"

namespace baidu {
namespace shuttle {

SortFileReader* SortFileReader::Create(FileType file_type, Status* status) {
    if (file_type == kHdfsFile) {
        *status = kOk;
        return new SortFileHdfsReader();
    } else {
        *status = kNotImplement;
        return NULL;
    }
}

SortFileWriter* SortFileWriter::Create(FileType file_type, Status* status) {
    if (file_type == kHdfsFile) {
        *status = kOk;
        return new SortFileHdfsWriter();
    } else {
        *status = kNotImplement;
        return NULL;
    }
}

}
}