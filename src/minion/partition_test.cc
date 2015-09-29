#include <gtest/gtest.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include "partition.h"

using namespace baidu::shuttle;

TEST(Partitioner, KeyBased) {
    TaskInfo task;
    task.mutable_job()->set_reduce_total(10);
    task.mutable_job()->set_key_separator(" ");
    task.mutable_job()->set_key_fields_num(2);
    task.mutable_job()->set_partition_fields_num(1);
    KeyFieldBasedPartitioner kf_parti(task);
    std::string key;
    int reduce_no = kf_parti.Calc("abc 555 zzzzz", &key);
    EXPECT_EQ(key, "abc 555");
    EXPECT_EQ(reduce_no, 5);
    reduce_no = kf_parti.Calc("sjy", &key);
    EXPECT_EQ(key, "sjy");
    EXPECT_EQ(reduce_no, 3);
    reduce_no = kf_parti.Calc("", &key);
    EXPECT_EQ(key, "");
    EXPECT_EQ(reduce_no, 0);
}


TEST(Partitioner, KeyBasedInvalid) {
    TaskInfo task;
    task.mutable_job()->set_reduce_total(100);
    task.mutable_job()->set_key_separator("\t");
    task.mutable_job()->set_key_fields_num(3);
    task.mutable_job()->set_partition_fields_num(1);
    KeyFieldBasedPartitioner kf_parti(task);
    std::string key;
    int reduce_no = kf_parti.Calc("k1\tk2", &key);
    EXPECT_EQ(key, "k1\tk2");
    EXPECT_EQ(reduce_no, 27);
}

TEST(Partitioner, KeyDefault) {
    TaskInfo task;
    task.mutable_job()->set_reduce_total(100);
    KeyFieldBasedPartitioner kf_parti(task);
    std::string key;
    int reduce_no = kf_parti.Calc("k1\tk2", &key);
    EXPECT_EQ(key, "k1");
    EXPECT_EQ(reduce_no, 27);
}

TEST(Partitioner, IntHash) {
    TaskInfo task;
    task.mutable_job()->set_reduce_total(100);
    task.mutable_job()->set_key_separator(" ");
    IntHashPartitioner ih_parti(task);
    std::string key;
    int reduce_no = ih_parti.Calc("17 key_123 value456", &key);
    EXPECT_EQ(key, "key_123");
    EXPECT_EQ(reduce_no, 17);
}

TEST(Partitioner, IntHashInValid) {
    TaskInfo task;
    task.mutable_job()->set_reduce_total(200);
    task.mutable_job()->set_key_separator(" ");
    IntHashPartitioner ih_parti(task);
    std::string key;
    int reduce_no = ih_parti.Calc("-1 key_123 value456", &key);
    EXPECT_EQ(key, "key_123");
    EXPECT_EQ(reduce_no, -1);
    reduce_no = ih_parti.Calc("", &key);
    EXPECT_EQ(reduce_no, 0);
    EXPECT_EQ(key, "");
    reduce_no = ih_parti.Calc("123 ", &key);
    EXPECT_EQ(reduce_no, 123);
    EXPECT_EQ(key, "");
    reduce_no = ih_parti.Calc("123 aaaaaaaaaaaaazzzzzzzz", &key);
    EXPECT_EQ(reduce_no, 123);
    EXPECT_EQ(key, "aaaaaaaaaaaaazzzzzzzz");
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
