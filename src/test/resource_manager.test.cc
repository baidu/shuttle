#include "master/resource_manager.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <boost/lexical_cast.hpp>
#include <cstdio>
#include "common/file.h"

using namespace baidu::shuttle;

/*
 * Test needs an address to an input file. The file must be big enough or have enough lines
 * Resource manager will stat the file and divide them into pieces logically, but the result
 *   needs to be validated by user
 */

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
        inputs.push_back(info);
        if (FLAGS_type == "block") {
            manager = ResourceManager::GetBlockManager(inputs, FLAGS_split_size);
        } else if (FLAGS_type == "line") {
            manager = ResourceManager::GetNLineManager(inputs);
        } else {
            ASSERT_TRUE(false);
        }
        ASSERT_TRUE(manager != NULL);
        ASSERT_EQ(manager->SumOfItems(), FLAGS_items);
    }

    virtual void TearDown() {
        if (manager != NULL) {
            delete manager;
        }
        manager = NULL;
    }
};

TEST_F(ResManTest, BasicTest) {
    int sum = manager->SumOfItems();
    EXPECT_EQ(sum, manager->Pending());
    EXPECT_EQ(manager->Allocated(), 0);
    EXPECT_EQ(manager->Done(), 0);
    int64_t last_offset = 0;
    std::string last_input_file;
    // Loop to exhaust items in pool
    for (int i = 0; i < sum; ++i) {
        EXPECT_TRUE(!manager->IsAllocated(i));
        EXPECT_TRUE(!manager->IsDone(i));
        ResourceItem* cur = manager->GetItem();
        if (last_input_file != cur->input_file) {
            last_input_file = cur->input_file;
            last_offset = 0;
        }
        EXPECT_EQ(cur->no, i);
        EXPECT_EQ(cur->attempt, 1);
        EXPECT_EQ(cur->offset, last_offset);
        EXPECT_TRUE(manager->IsAllocated(i));
        EXPECT_TRUE(!manager->IsDone(i));
        last_offset = cur->offset + cur->size;
        delete cur;
        EXPECT_EQ(manager->Pending(), sum - i - 1);
        EXPECT_EQ(manager->Allocated(), i + 1);
        EXPECT_EQ(manager->Done(), 0);
    }
    EXPECT_EQ(manager->Pending(), 0);
    EXPECT_EQ(manager->Allocated(), manager->SumOfItems());
    EXPECT_EQ(manager->Done(), 0);

    // Loop to finish items
    for (int i = 0; i < sum; ++i) {
        EXPECT_TRUE(manager->IsAllocated(i));
        EXPECT_TRUE(!manager->IsDone(i));
        manager->FinishItem(i);
        EXPECT_TRUE(!manager->IsAllocated(i));
        EXPECT_TRUE(manager->IsDone(i));
        EXPECT_EQ(manager->Pending(), 0);
        EXPECT_EQ(manager->Allocated(), sum - i - 1);
        EXPECT_EQ(manager->Done(), i + 1);
    }
    EXPECT_EQ(manager->Pending(), 0);
    EXPECT_EQ(manager->Allocated(), 0);
    EXPECT_EQ(manager->Done(), manager->SumOfItems());
}

TEST_F(ResManTest, GetCheckCertainItemTest) {
    int64_t last_size = 0;

    EXPECT_TRUE(!manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));
    // Throw away the first item
    delete manager->GetItem();
    EXPECT_TRUE(manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));

    // Get certain item for 3 times
    for (int i = 2; i < 5; ++i) {
        ResourceItem* cur = manager->GetCertainItem(0);
        EXPECT_EQ(cur->no, 0);
        EXPECT_EQ(cur->attempt, i);
        EXPECT_EQ(cur->offset, 0);
        EXPECT_TRUE(last_size == 0 || cur->size == last_size);
        last_size = cur->size;
        delete cur;
    }
    EXPECT_TRUE(manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));

    // Check certain item for a few times
    for (int i = 2; i < 5; ++i) {
        ResourceItem* cur = manager->CheckCertainItem(0);
        EXPECT_EQ(cur->no, 0);
        EXPECT_EQ(cur->attempt, 4);
        EXPECT_EQ(cur->offset, 0);
        EXPECT_TRUE(last_size == 0 || cur->size == last_size);
        last_size = cur->size;
        delete cur;
    }
    EXPECT_TRUE(manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));
}

TEST_F(ResManTest, ReturnBackItemTest) {
    int64_t last_end = 0;
    std::string last_input_file;

    EXPECT_TRUE(!manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));
    // Get first item
    ResourceItem* cur = manager->GetItem();
    EXPECT_TRUE(manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->offset, 0);
    last_end = cur->size;
    last_input_file = cur->input_file;
    delete cur;

    EXPECT_TRUE(!manager->IsAllocated(1));
    EXPECT_TRUE(!manager->IsDone(1));
    // Get second item
    cur = manager->GetItem();
    EXPECT_TRUE(manager->IsAllocated(1));
    EXPECT_TRUE(!manager->IsDone(1));
    if (cur->input_file != last_input_file) {
        last_end = 0;
    }
    EXPECT_EQ(cur->no, 1);
    EXPECT_EQ(cur->attempt, 1);
    EXPECT_EQ(cur->offset, last_end);
    delete cur;

    // Return the first item
    EXPECT_TRUE(manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));
    manager->ReturnBackItem(0);
    EXPECT_TRUE(!manager->IsAllocated(0));
    EXPECT_TRUE(!manager->IsDone(0));
    cur = manager->GetItem();
    EXPECT_EQ(cur->no, 0);
    EXPECT_EQ(cur->attempt, 2);
    EXPECT_EQ(cur->offset, 0);
    EXPECT_EQ(cur->input_file, last_input_file);
    delete cur;
}

TEST_F(ResManTest, LoadDumpTest) {
    int sum = manager->SumOfItems();
    for (int i = 0; i < sum / 2; ++i) {
        EXPECT_TRUE(!manager->IsAllocated(i));
        EXPECT_TRUE(!manager->IsDone(i));
        delete manager->GetItem();
        EXPECT_TRUE(manager->IsAllocated(i));
        EXPECT_TRUE(!manager->IsDone(i));
    }

    const std::vector<ResourceItem>& data = manager->Dump();
    ResourceManager* copy = ResourceManager::BuildManagerFromBackup(data);
    ASSERT_EQ(sum, manager->SumOfItems());
    ASSERT_EQ(copy->SumOfItems(), manager->SumOfItems());
    for (int i = 0; i < sum; ++i) {
        ResourceItem* origin_item = manager->CheckCertainItem(i);
        ResourceItem* copy_item = copy->CheckCertainItem(i);
        EXPECT_EQ(origin_item->type, copy_item->type);
        EXPECT_EQ(origin_item->no, copy_item->no);
        EXPECT_EQ(origin_item->attempt, copy_item->attempt);
        EXPECT_EQ(origin_item->status, copy_item->status);
        EXPECT_EQ(origin_item->input_file, copy_item->input_file);
        EXPECT_EQ(origin_item->offset, copy_item->offset);
        EXPECT_EQ(origin_item->size, copy_item->size);
        EXPECT_EQ(origin_item->allocated, copy_item->allocated);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}

