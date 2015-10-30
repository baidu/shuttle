#include "resource_manager.h"

#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>
#include <cstdio>

using namespace baidu::shuttle;

std::string hdfs_path = "hdfs://0.0.0.0:0/users/";
int sum_of_items = 0;

TEST(ResManTest, SetInputFilesTest) {
    std::vector<std::string> input_files;
    input_files.push_back(hdfs_path);
    FileSystem::Param p;
    ResourceManager resman(input_files, p);
    EXPECT_EQ(resman.SumOfItem(), sum_of_items);
}

TEST(ResManTest, SetNLineFileTest) {
    // TODO separate from SetInputFileTest
    ResourceManager resman;
    resman.SetNLineFile(hdfs_path);
    EXPECT_EQ(resman.SumOfItem(), sum_of_items);
}

TEST(ResManTest, GetItemTest) {
    std::string input_file = hdfs_path;
    std::vector<std::string> input_files;
    input_files.push_back(input_file);
    FileSystem::Param p;
    ResourceManager resman(input_files, p);
    int sum = resman.SumOfItem();
    int64_t last_offset = 0;
    for (int i = 0; i < sum; ++i) {
        ResourceItem* cur = resman.GetItem();
        EXPECT_EQ(cur->no, i);
        EXPECT_EQ(cur->attempt, 1);
        EXPECT_EQ(cur->input_file, input_file);
        EXPECT_EQ(cur->offset, last_offset);
        last_offset = cur->offset + cur->size;
        delete cur;
    }
}

TEST(ResManTest, GetCertainItemTest) {
    std::string input_file = hdfs_path;
    std::vector<std::string> input_files;
    input_files.push_back(input_file);
    FileSystem::Param p;
    ResourceManager resman(input_files, p);
    int sum = resman.SumOfItem();
    int64_t last_size = 0;
    delete resman.GetItem();
    for (int i = 2; i < sum + 2; ++i) {
        ResourceItem* cur = resman.GetCertainItem(0);
        EXPECT_EQ(cur->no, 0);
        EXPECT_EQ(cur->attempt, i);
        EXPECT_EQ(cur->input_file, input_file);
        EXPECT_EQ(cur->offset, 0);
        EXPECT_TRUE(last_size == 0 || cur->size == last_size);
        last_size = cur->size;
        delete cur;
    }
}

TEST(ResManTest, ReturnBackItemTest) {
    std::string input_file = hdfs_path;
    std::vector<std::string> input_files;
    input_files.push_back(input_file);
    FileSystem::Param p;
    ResourceManager resman(input_files, p);
    int64_t last_end = 0;
    ResourceItem* cur = resman.GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->input_file, input_file);
    EXPECT_EQ(cur->offset, 0);
    last_end = cur->size;
    delete cur;
    cur = resman.GetItem();
    EXPECT_EQ(cur->no, 1);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->input_file, input_file);
    EXPECT_EQ(cur->offset, last_end);
    delete cur;
    resman.ReturnBackItem(0);
    cur = resman.GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 2);
    EXPECT_EQ(cur->input_file, input_file);
    EXPECT_EQ(cur->offset, 0);
    delete cur;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: resman_test [hdfs work dir] [sum of items]\n");
        return -1;
    }
    hdfs_path = argv[1];
    sum_of_items = boost::lexical_cast<int>(argv[2]);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

