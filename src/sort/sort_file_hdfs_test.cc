#include <gtest/gtest.h>
#include <string>
#include "sort_file.h"

using namespace baidu::shuttle;

std::string g_work_dir = "/tmp";
int total = 7500000;

TEST(HdfsTest, Put) {
    Status status;
    SortFileWriter* writer = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    status = writer->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    char key[256] = {'\0'};
    char value[256] = {'\0'};
    for (int i = 1; i <= total; i++) {
        snprintf(key, sizeof(key), "key_%09d", i);
        snprintf(value, sizeof(value), "value_%d", i*2);
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

TEST(HdfsTest, Put2) {
    Status status;
    SortFileWriter* writer = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter::Param param;
    std::string file_path = g_work_dir + "/put_test2.data";
    status = writer->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    char key[256] = {'\0'};
    char value[256] = {'\0'};
    for (int i = 1; i <= 250; i++) {
        snprintf(key, sizeof(key), "key_%09d", i);
        snprintf(value, sizeof(value), "value_%d", i*2);
        status = writer->Put(key, value);
        EXPECT_EQ(status, kOk);
    }
    status = writer->Close();
    EXPECT_EQ(status, kOk);
    printf("done\n");
}

TEST(HdfsTest, PutSameKey) {
    Status status;
    SortFileWriter* writer = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter::Param param;
    std::string file_path = g_work_dir + "/put_test_same.data";
    status = writer->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    char value[256] = {'\0'};
    writer->Put("abc", "123");
    for (int i = 1; i <= 100000; i++) {
        snprintf(value, sizeof(value), "value_%d", i*2);
        status = writer->Put("key_999", value);
        EXPECT_EQ(status, kOk);
    }
    status = writer->Close();
    EXPECT_EQ(status, kOk);
    printf("done\n");
}

TEST(HdfsTest, Read) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_000000123", "key_000001123");
    EXPECT_EQ(it->Error(), kOk);
    int n = 123;
    while (!it->Done()) {
        char key[256];
        snprintf(key, sizeof(key), "key_%09d", n);
        EXPECT_EQ(it->Key(), std::string(key));
        it->Next();
        n++;
    }
    EXPECT_EQ(it->Error(), kOk);
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(n, 1123);
    delete it;
}

TEST(HdfsTest, Read2) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("", "");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, total);
    delete it;
}

TEST(HdfsTest, Read3) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("a", "b");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 0);
    delete it;
}

TEST(HdfsTest, Read4) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_000008888", "key_000018888");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 10000);
    delete it;
}

TEST(HdfsTest, Read5) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("a", "key_000000012");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 11);
    delete it;
}

TEST(HdfsTest, Read6) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test2.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("a", "key_000000012");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        printf("%s -> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 11);
    delete it;
}

TEST(HdfsTest, Read7) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test2.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_000000001", "key_000000012");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        printf("%s -> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 11);
    delete it;
}

TEST(HdfsTest, Read8) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test2.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_000000249", "");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        printf("%s -> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 2);
}

TEST(HdfsTest, Read9) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test2.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_000000251", "");
    EXPECT_EQ(it->Error(), kNoMore);
    int count = 0;
    while (!it->Done()) {
        count++;
        printf("%s -> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
    }
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 0);
    delete it;
}

TEST(HdfsTest, Read10) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test2.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_000000251", "key_000000250");
    EXPECT_EQ(it->Error(), kInvalidArg);
    int count = 0;
    while (!it->Done()) {
        count++;
        printf("%s -> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
    }
    EXPECT_EQ(it->Error(), kInvalidArg);
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 0);
    delete it;
}

TEST(HdfsTest, ReadSameKey) {
    Status status;
    SortFileReader* reader = SortFileReader::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileReader::Param param;
    std::string file_path = g_work_dir + "/put_test_same.data";
    status = reader->Open(file_path, param);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator *it = reader->Scan("key_999", "key_999z");
    EXPECT_EQ(it->Error(), kOk);
    int count = 0;
    while (!it->Done()) {
        count++;
        it->Next();
    }
    EXPECT_TRUE(it->Error() == kOk || it->Error() == kNoMore);
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    EXPECT_EQ(count, 100000);
    delete it;
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
