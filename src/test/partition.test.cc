#include "minion/output/partition.h"

#include <gtest/gtest.h>

#include <string>
#include <stdlib.h>
#include <stdio.h>

using namespace baidu::shuttle;

/*
 * Test will run without any parameters
 */

const int dest_num = 100;

TEST(KeyBasedPartitionerTest, NormalTest) {
    std::string separator(" ");
    int key_fields = 2;
    int partition_fields = 1;

    Partitioner* partitioner = Partitioner::Get(
            kKeyFieldBasedPartitioner, separator,
            key_fields, partition_fields, dest_num);

    std::string key;
    // Normal case
    int dest = partitioner->Calc("key1 key2 value", &key);
    EXPECT_EQ(key, "key1 key2");
    EXPECT_EQ(dest, partitioner->HashCode("key1") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("key1"));
    // Column is less than key fields number
    dest = partitioner->Calc("line", &key);
    EXPECT_EQ(key, "line");
    EXPECT_EQ(dest, partitioner->HashCode("line") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("line"));
    // Empty string
    dest = partitioner->Calc("", &key);
    EXPECT_EQ(key, "");
    EXPECT_EQ(dest, 0);
    EXPECT_EQ(dest, partitioner->Calc(""));
    // No such separator
    dest = partitioner->Calc("key1\tkey2", &key);
    EXPECT_EQ(key, "key1\tkey2");
    EXPECT_EQ(dest, partitioner->HashCode("key1\tkey2") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("key1\tkey2"));

    delete partitioner;
}

TEST(KeyBasedPartitionerTest, HighFieldsTest) {
    std::string separator(" ");
    int key_fields = 2;
    int partition_fields = 3;

    Partitioner* partitioner = Partitioner::Get(
            kKeyFieldBasedPartitioner, separator,
            key_fields, partition_fields, dest_num);

    std::string key;
    // Exact case
    int dest = partitioner->Calc("key1 key2 key3 value", &key);
    EXPECT_EQ(key, "key1 key2");
    EXPECT_EQ(dest, partitioner->HashCode("key1 key2 key3") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("key1 key2 key3"));
    // Column is less than partition fields number
    dest = partitioner->Calc("col1 col2", &key);
    EXPECT_EQ(key, "col1 col2");
    EXPECT_EQ(dest, partitioner->HashCode("col1 col2") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("col1 col2"));

    delete partitioner;
}

TEST(PartitionerTest, DefaultValueTest) {
    Partitioner* partitioner = Partitioner::Get(
            kKeyFieldBasedPartitioner, "", 0, 0, dest_num);

    std::string key;
    // Default separator: \t
    int dest = partitioner->Calc("key1\tkey2", &key);
    EXPECT_EQ(key, "key1");
    EXPECT_EQ(dest, partitioner->HashCode("key1") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("key1"));
    // Default key fields: 1, partition fields: 1
    dest = partitioner->Calc("col1\tcol2\tcol3\tcol4", &key);
    EXPECT_EQ(key, "col1");
    EXPECT_EQ(dest, partitioner->HashCode("col1") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("col1"));

    delete partitioner;
}

TEST(IntHashPartitionerTest, PartitionTest) {
    std::string separator("\t");
    // Only key separator is needed
    Partitioner* partitioner = Partitioner::Get(
            kIntHashPartitioner, separator, 0, 0, dest_num);

    std::string key;
    // Normal case
    int dest = partitioner->Calc("17 key\tvalue", &key);
    EXPECT_EQ(key, "key");
    EXPECT_EQ(dest, 17);
    EXPECT_EQ(dest, partitioner->Calc("17 key"));
    // Empty string
    dest = partitioner->Calc("", &key);
    EXPECT_EQ(key, "");
    EXPECT_EQ(dest, 0);
    EXPECT_EQ(dest, partitioner->Calc(""));
    // No int separator: ' '(even it IS a number)
    dest = partitioner->Calc("17", &key);
    EXPECT_EQ(key, "17");
    EXPECT_EQ(dest, partitioner->HashCode("17") % dest_num);
    EXPECT_EQ(dest, partitioner->Calc("17"));
    // Negative column number: use atoi inside, don't care
    dest = partitioner->Calc("-1 key\tvalue", &key);
    EXPECT_EQ(key, "key");
    EXPECT_EQ(dest, -1);
    EXPECT_EQ(dest, partitioner->Calc("-1 key"));
    // Column number only
    dest = partitioner->Calc("17 ", &key);
    EXPECT_EQ(key, "");
    EXPECT_EQ(dest, 17);
    EXPECT_EQ(dest, partitioner->Calc("17 "));
    // Invalid int column
    dest = partitioner->Calc("invalid key\tvalue", &key);
    EXPECT_EQ(key, "key");
    EXPECT_EQ(dest, 0);
    EXPECT_EQ(dest, partitioner->Calc("invalid key"));
    // More spaces
    dest = partitioner->Calc("17 key \t value", &key);
    EXPECT_EQ(key, "key ");
    EXPECT_EQ(dest, 17);
    EXPECT_EQ(dest, partitioner->Calc("17 key "));
    // Empty key
    dest = partitioner->Calc("17 \tcol2\tcol3", &key);
    EXPECT_EQ(key, "");
    EXPECT_EQ(dest, 17);
    EXPECT_EQ(dest, partitioner->Calc("17 "));

    delete partitioner;
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

