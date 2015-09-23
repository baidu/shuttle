#include <gtest/gtest.h>
#include <string>
#include "sort_file.h"

using namespace baidu::shuttle;

std::string g_work_dir = "/tmp";

TEST(Merge, Put1) {
    Status status;
    SortFileWriter* writer1 = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter* writer2 = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter* writer3 = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);

    SortFileWriter::Param param;
    std::string file_path_1 = g_work_dir + "/merge_test1.data";
    status = writer1->Open(file_path_1, param);
    EXPECT_EQ(status, kOk);
    std::string file_path_2 = g_work_dir + "/merge_test2.data";
    status = writer2->Open(file_path_2, param);
    EXPECT_EQ(status, kOk);
    std::string file_path_3 = g_work_dir + "/merge_test3.data";
    status = writer3->Open(file_path_3, param);
    EXPECT_EQ(status, kOk);

    char key[256] = {'\0'};
    char value[256] = {'\0'};
    for (int i = 1; i <= 3; i++) {
        snprintf(key, sizeof(key), "key_%09d", i);
        snprintf(value, sizeof(value), "value_%d", i*2);
        if (i % 2 == 0) {
            status = writer1->Put(key, value);
            EXPECT_EQ(status, kOk);
        }
        if (i % 3 == 0) {
            status = writer2->Put(key, value);
            EXPECT_EQ(status, kOk);
        }
        if (i % 6 == 0) {
            status = writer3->Put(key, value);
            EXPECT_EQ(status, kOk);
        }
    }

    status = writer1->Close();
    EXPECT_EQ(status, kOk);
    status = writer2->Close();
    EXPECT_EQ(status, kOk);
    status = writer3->Close();
    EXPECT_EQ(status, kOk);

    printf("done\n");
}

TEST(Merge, Read1) {
    MergeFileReader* reader = new MergeFileReader();
    std::string file_path_1 = g_work_dir + "/merge_test1.data";
    std::string file_path_2 = g_work_dir + "/merge_test2.data";
    std::string file_path_3 = g_work_dir + "/merge_test3.data";
    std::vector<std::string> file_names;
    file_names.push_back(file_path_1);
    file_names.push_back(file_path_2);
    file_names.push_back(file_path_3);
    SortFileReader::Param param;
    Status status = reader->Open(file_names, param, kHdfsFile);
    EXPECT_EQ(status, kOk);
    SortFileReader::Iterator* it = reader->Scan("", "");
    while (!it->Done()) {
        printf("%s --> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
        EXPECT_EQ(it->Error(), kOk);
    }
    delete it;
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    delete reader;    
}


TEST(Merge, Put2) {
    Status status;
    SortFileWriter* writer1 = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter* writer2 = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);
    SortFileWriter* writer3 = SortFileWriter::Create(kHdfsFile, &status);
    EXPECT_EQ(status, kOk);

    SortFileWriter::Param param;
    std::string file_path_1 = g_work_dir + "/merge_test1.data";
    status = writer1->Open(file_path_1, param);
    EXPECT_EQ(status, kOk);
    std::string file_path_2 = g_work_dir + "/merge_test2.data";
    status = writer2->Open(file_path_2, param);
    EXPECT_EQ(status, kOk);
    std::string file_path_3 = g_work_dir + "/merge_test3.data";
    status = writer3->Open(file_path_3, param);
    EXPECT_EQ(status, kOk);

    char key[256] = {'\0'};
    char value[256] = {'\0'};
    for (int i = 1; i <= 750000; i++) {
        snprintf(key, sizeof(key), "key_%09d", i);
        snprintf(value, sizeof(value), "value_%d", i*2);
        if (i % 2 == 0) {
            status = writer1->Put(key, value);
            EXPECT_EQ(status, kOk);
        }
        if (i % 2 == 1) {
            status = writer2->Put(key, value);
            EXPECT_EQ(status, kOk);
        }
        if (i % 3 == 0) {
            status = writer3->Put(key, value);
            EXPECT_EQ(status, kOk);
        }
    }

    status = writer1->Close();
    EXPECT_EQ(status, kOk);
    status = writer2->Close();
    EXPECT_EQ(status, kOk);
    status = writer3->Close();
    EXPECT_EQ(status, kOk);

    printf("done\n");
}

TEST(Merge, Read2) {
    MergeFileReader* reader = new MergeFileReader();
    std::string file_path_1 = g_work_dir + "/merge_test1.data";
    std::string file_path_2 = g_work_dir + "/merge_test2.data";
    std::string file_path_3 = g_work_dir + "/merge_test3.data";
    std::vector<std::string> file_names;
    file_names.push_back(file_path_1);
    file_names.push_back(file_path_2);
    file_names.push_back(file_path_3);
    SortFileReader::Param param;
    Status status = reader->Open(file_names, param, kHdfsFile);
    EXPECT_EQ(status, kOk);
    int ct = 0;
    SortFileReader::Iterator* it = reader->Scan("", "");
    while (!it->Done()) {
        //printf("%s --> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
        EXPECT_EQ(it->Error(), kOk);
        ct++;
    }
    delete it;
    EXPECT_EQ(ct, 1000000);
    ct = 0;
    it = reader->Scan("key_000010000", "key_000020000");
    while (!it->Done()) {
        //printf("%s --> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
        EXPECT_EQ(it->Error(), kOk);
        ct++;
    }
    delete it;
    EXPECT_EQ(ct, 13333);
    ct = 0;
    it = reader->Scan("key_000080000", "key_000090000");
    while (!it->Done()) {
        //printf("%s --> %s\n", it->Key().c_str(), it->Value().c_str());
        it->Next();
        EXPECT_EQ(it->Error(), kOk);
        ct++;
    }
    delete it;
    EXPECT_EQ(ct, 13333);
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    delete reader;    
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("./merge_test [hdfs work dir]\n");
        return -1;
    }
    g_work_dir = argv[1];
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
