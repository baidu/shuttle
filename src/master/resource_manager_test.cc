#include "resource_manager.h"

#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>
#include <cstdio>

using namespace baidu::shuttle;

std::vector<std::string> input_files;
const int64_t split_size = 500l * 1024 * 1024;
int sum_of_items = 0;

TEST(ResManTest, SetInputFilesTest) {
    FileSystem::Param p;
    ResourceManager* resman = ResourceManager::GetBlockManager(input_files, p, split_size);
    EXPECT_EQ(resman->SumOfItem(), sum_of_items);
}

/*TEST(ResManTest, NLineFileTest) {
    FileSystem::Param p;
    ResourceManager* resman = GetNLineManager(input_files, p);
    EXPECT_EQ(resman->SumOfItem(), sum_of_items);
}*/

TEST(ResManTest, GetItemTest) {
    FileSystem::Param p;
    ResourceManager* resman = ResourceManager::GetBlockManager(input_files, p, split_size);
    int sum = resman->SumOfItem();
    int64_t last_offset = 0;
    std::string last_input_file;
    for (int i = 0; i < sum; ++i) {
        ResourceItem* cur = resman->GetItem();
        if (last_input_file != cur->input_file) {
            last_input_file = cur->input_file;
            last_offset = 0;
        }
        EXPECT_EQ(cur->no, i);
        EXPECT_EQ(cur->attempt, 1);
        EXPECT_EQ(cur->offset, last_offset);
        last_offset = cur->offset + cur->size;
        delete cur;
    }
}

TEST(ResManTest, GetCertainItemTest) {
    FileSystem::Param p;
    ResourceManager* resman = ResourceManager::GetBlockManager(input_files, p, split_size);
    int64_t last_size = 0;
    delete resman->GetItem();
    for (int i = 2; i < 5; ++i) {
        ResourceItem* cur = resman->GetCertainItem(0);
        EXPECT_EQ(cur->no, 0);
        EXPECT_EQ(cur->attempt, i);
        EXPECT_EQ(cur->offset, 0);
        EXPECT_TRUE(last_size == 0 || cur->size == last_size);
        last_size = cur->size;
        delete cur;
    }
}

TEST(ResManTest, ReturnBackItemTest) {
    FileSystem::Param p;
    ResourceManager* resman = ResourceManager::GetBlockManager(input_files, p, split_size);
    int64_t last_end = 0;
    std::string last_input_file;
    ResourceItem* cur = resman->GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->offset, 0);
    last_end = cur->size;
    last_input_file = cur->input_file;
    delete cur;
    cur = resman->GetItem();
    if (cur->input_file != last_input_file) {
        last_end = 0;
    }
    EXPECT_EQ(cur->no, 1);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->offset, last_end);
    delete cur;
    resman->ReturnBackItem(0);
    cur = resman->GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 2);
    EXPECT_EQ(cur->offset, 0);
    EXPECT_EQ(cur->input_file, last_input_file);
    delete cur;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: resman_test [hdfs work dir] [sum of items]\n");
        return -1;
    }
    for (int i = 1; i < argc - 1; ++i) {
        input_files.push_back(argv[i]);
    }
    sum_of_items = boost::lexical_cast<int>(argv[argc - 1]);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

