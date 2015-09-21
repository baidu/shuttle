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
    char key[256] = {'\0'};
    char value[256] = {'\0'};
    int total = 250000;
    for (int i = total; i >=1; i--) {
        snprintf(key, sizeof(key), "key_%i", i);
        snprintf(value, sizeof(value), "value_%i", i*2);
        status = writer->Put(key, value);
        EXPECT_EQ(status, kOk);
        if (i % 1000 == 0) {
            printf("%d / %d\n", i , total);
        }
    }
    status = writer->Close();
    EXPECT_EQ(status, kOk);
    printf("done\n");
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
