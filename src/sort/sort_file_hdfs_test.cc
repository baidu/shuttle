#include <gtest/gtest.h>
#include "sort_file_hdfs.h"

using namespace baidu::shuttle;

TEST(HdfsTest, Put) {
    SortFileWriter* writer = new SortFileHdfsWriter();
    SortFileWriter::Param param;
    Status status = writer->Open("/tmp/put_test.data", param);
    EXPECT_EQ(status, kOk);
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
