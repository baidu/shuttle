#include "resource_manager.h"

#include <gtest/gtest.h>
#include <cstdio>

using namespace baidu::shuttle;

std::string hdfs_path = "hdfs://0.0.0.0:0/users/";

TEST(ResManTest, SetInputFilesTest) {
    ResourceManager resman;
    std::vector<std::string> input_files;
    input_files.push_back(hdfs_path + "1.txt");
    resman.SetInputFiles(input_files);
    EXPECT_TRUE(resman.SumOfItem() == 30);
}

TEST(ResManTest, GetItemTest) {
    ResourceManager resman;
    std::string input_file = hdfs_path + "1.txt";
    std::vector<std::string> input_files;
    input_files.push_back(input_file);
    resman.SetInputFiles(input_files);
    int sum = resman.SumOfItem();
    int last_offset = 0;
    for (int i = 0; i < sum; ++i) {
        ResourceItem* cur = resman.GetItem();
        EXPECT_TRUE(cur->no == i);
        EXPECT_TRUE(cur->attempt == 0);
        EXPECT_TRUE(cur->input_file == input_file);
        EXPECT_TRUE(cur->offset == last_offset);
        last_offset = cur->offset + cur->size;
        delete cur;
    }
}

TEST(ResManTest, GetCertainItemTest) {
    ResourceManager resman;
    std::string input_file = hdfs_path + "1.txt";
    std::vector<std::string> input_files;
    input_files.push_back(input_file);
    resman.SetInputFiles(input_files);
    int sum = resman.SumOfItem();
    int last_size = 0;
    for (int i = 0; i < sum; ++i) {
        ResourceItem* cur = resman.GetCertainItem(0);
        EXPECT_TRUE(cur->no == 0);
        EXPECT_TRUE(cur->attempt == i);
        EXPECT_TRUE(cur->input_file == input_file);
        EXPECT_TRUE(cur->offset == 0);
        EXPECT_TRUE(cur->size == 0 || cur->size == last_size);
        last_size = cur->size;
        delete cur;
    }
}

TEST(ResManTest, ReturnBackItemTest) {
    ResourceManager resman;
    std::string input_file = hdfs_path + "1.txt";
    std::vector<std::string> input_files;
    input_files.push_back(input_file);
    resman.SetInputFiles(input_files);
    ResourceItem* cur = resman.GetItem();
    EXPECT_TRUE(cur->no == 0);
    EXPECT_TRUE(cur->attempt == 0);
    EXPECT_TRUE(cur->input_file == input_file);
    EXPECT_TRUE(cur->offset == 0);
    delete cur;
    cur = resman.GetItem();
    EXPECT_TRUE(cur->no == 1);
    EXPECT_TRUE(cur->attempt == 0);
    EXPECT_TRUE(cur->input_file == input_file);
    EXPECT_TRUE(cur->offset == 0);
    delete cur;
    resman.ReturnBackItem(0);
    cur = resman.GetItem();
    EXPECT_TRUE(cur->no == 0);
    EXPECT_TRUE(cur->attempt == 1);
    EXPECT_TRUE(cur->input_file == input_file);
    EXPECT_TRUE(cur->offset == 0);
    delete cur;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: resman_test [hdfs work dir]\n");
        return -1;
    }
    hdfs_path = argv[1];
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

