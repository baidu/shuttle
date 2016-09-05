#include "master/resource_manager.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <boost/lexical_cast.hpp>
#include <cstdio>
#include "common/file.h"

using namespace baidu::shuttle;

DEFINE_string(type, "block", "set resource type, block/line is acceptable");
DEFINE_string(address, "", "full address of the test file");
DEFINE_string(user, "", "username to FS, empty meanse default");
DEFINE_string(password, "", "password to FS, empty only when username is empty");
DEFINE_int32(items, 0, "sum of items, to validate the output of resource manager");
DEFINE_int64(split_size, 500l * 1024 * 1024, "split size of block resource manager");

// Global pointer for ResManTest, will be automatically initialized and released
static ResourceManager* manager = NULL;

class ResManTest : public testing::Test {
protected:
    virtual void SetUp() {
        if (manager != NULL) {
            return;
        }
        ASSERT_TRUE(FLAGS_address != "");
        DfsInfo info;
        info.set_path(FLAGS_address);
        if (FLAGS_user != "") {
            info.set_user(FLAGS_user);
        }
        if (FLAGS_password != "") {
            info.set_password(FLAGS_password);
        }
        std::vector<DfsInfo> inputs;
        if (FLAGS_type == "block") {
            inputs.push_back(info);
            manager = ResourceManager::GetBlockManager(inputs, FLAGS_split_size);
        } else if (FLAGS_type == "line") {
            manager = ResourceManager::GetNLineManager(inputs);
        } else {
            ASSERT_TRUE(false);
        }
        ASSERT_TRUE(manager != NULL);
        ASSERT_EQ(manager->SumOfItem(), FLAGS_items);
    }

    virtual void TearDown() {
        if (manager != NULL) {
            delete manager;
        }
        manager = NULL;
    }
};

TEST_F(ResManTest, GetItemTest) {
    int sum = manager->SumOfItem();
    int64_t last_offset = 0;
    std::string last_input_file;
    for (int i = 0; i < sum; ++i) {
        ResourceItem* cur = manager->GetItem();
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

TEST_F(ResManTest, GetCertainItemTest) {
    int64_t last_size = 0;
    delete manager->GetItem();
    for (int i = 2; i < 5; ++i) {
        ResourceItem* cur = manager->GetCertainItem(0);
        EXPECT_EQ(cur->no, 0);
        EXPECT_EQ(cur->attempt, i);
        EXPECT_EQ(cur->offset, 0);
        EXPECT_TRUE(last_size == 0 || cur->size == last_size);
        last_size = cur->size;
        delete cur;
    }
}

TEST_F(ResManTest, ReturnBackItemTest) {
    int64_t last_end = 0;
    std::string last_input_file;
    ResourceItem* cur = manager->GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->offset, 0);
    last_end = cur->size;
    last_input_file = cur->input_file;
    delete cur;
    cur = manager->GetItem();
    if (cur->input_file != last_input_file) {
        last_end = 0;
    }
    EXPECT_EQ(cur->no, 1);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->offset, last_end);
    delete cur;
    manager->ReturnBackItem(0);
    cur = manager->GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 2);
    EXPECT_EQ(cur->offset, 0);
    EXPECT_EQ(cur->input_file, last_input_file);
    delete cur;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

