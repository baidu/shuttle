#include <gtest/gtest.h>
#include <string>
#include "sort_file_hdfs.h"

using namespace baidu::shuttle;

std::string g_work_dir = "/tmp";

TEST(HdfsTest, Put) {
    SortFileWriter* writer = new SortFileHdfsWriter();
    SortFileWriter::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    Status status = writer->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    status = writer->Close();
    EXPECT_EQ(status, kOk);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("./sort_test [hdfs work dir]\n");
        return -1;
    }
    g_work_dir = argv[1];
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
