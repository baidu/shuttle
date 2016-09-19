#include "minion/input/merger.h"

#include <gtest/gtest.h>
#include <sstream>
#include <cstdlib>
#include <cstdio>

using namespace baidu::shuttle;

/*
 * Merger test is not designed to test underlying format file or scanner, so it uses
 *   local file system and trust scanner/formattedfile to provide meaningful result
 * Test needs an empty or no important files containing directory, and will automatically create
 *   testcase files. But it WILL NOT delete the files itself and need maunal operation
 */

static std::string work_dir;
const int file_num = 10;

TEST(MergerTest, MergeTest) {
    // ----- Generate test data -----
    std::vector<FormattedFile*> files;
    File::Param param;
    // Open some test files
    for (int i = 0; i < file_num; ++i) {
        FormattedFile* fp = FormattedFile::Create(kLocalFs, kInternalSortedFile, param);
        ASSERT_TRUE(fp != NULL);
        std::stringstream ss;
        ss << work_dir << "merger_test_file_" << i;
        ASSERT_TRUE(fp->Open(ss.str(), kWriteFile, param));
        files.push_back(fp);
    }
    // Write data
    std::string key("key");
    std::string value("value");
    for (int i = 0; i < 100000; ++i) {
        std::stringstream ss;
        ss << std::setw(6) << std::setfill('0') << i;
        files[i % file_num]->WriteRecord(key + ss.str(), value + ss.str());
    }
    for (int i = 0; i < file_num; ++i) {
        ASSERT_TRUE(files[i]->Close());
        std::stringstream ss;
        ss << work_dir << "merger_test_file_" << i;
        ASSERT_TRUE(files[i]->Open(ss.str(), kReadFile, param));
    }

    // ----- Build merger -----
    Merger merger(files);
    Scanner::Iterator* it = merger.Scan(Scanner::SCAN_KEY_BEGINNING, Scanner::SCAN_ALL_KEY);
    int counter = 0;
    for (; !it->Done(); it->Next()) {
        EXPECT_EQ(it->Error(), kOk);
        EXPECT_EQ(it->GetFileName(), "");
        const std::string& key = it->Key();
        const std::string& value = it->Value();
        EXPECT_TRUE(key.substr(0, 3) == "key");
        EXPECT_TRUE(value.substr(0, 5) == "value");
        int cur_no_key = atoi(key.substr(3).c_str());
        int cur_no_value = atoi(value.substr(5).c_str());
        EXPECT_EQ(cur_no_key, cur_no_value);
        EXPECT_EQ(cur_no_key, counter);
        ++counter;
    }
    EXPECT_EQ(it->Key(), "key099999");
    EXPECT_EQ(it->Value(), "value099999");
    EXPECT_EQ(it->Error(), kNoMore);
    delete it;

    // ----- Release file resources -----
    for (int i = 0; i < file_num; ++i) {
        delete files[i];
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    if (argc != 2) {
        fprintf(stderr, "Usgae: merger_test /path/to/a/available/dir");
        return -1;
    }
    work_dir = argv[1];
    if (*work_dir.rbegin() != '/') {
        work_dir.push_back('/');
    }
    return RUN_ALL_TESTS();
}

