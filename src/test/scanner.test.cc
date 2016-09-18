#include "common/scanner.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "common/fileformat.h"
#include <cstdlib>

using namespace baidu::shuttle;

/*
 * Test needs an address to an inexist location, and will automatically create testcase file
 * But this test is not responsible for destroying test file and need manual operation
 * Scanner and underlying FormattedFile doesn't provide delete interface
 *   so it needs user to delete it
 */

DEFINE_string(type, "local", "set file type, local/hdfs is acceptable");
DEFINE_string(format, "text", "set file format, text/seq/sort is acceptable");
DEFINE_string(address, "", "full address to the test file");
DEFINE_string(user, "", "username to FS, empty means default");
DEFINE_string(password, "", "password to FS, empty only when username is empty");

// Common configuration for building a scanner
FileType type = kLocalFs;
FileFormat format = kPlainText;
ScannerType scantype = kInputScanner;
File::Param param;

void FillParam(File::Param& param) {
    std::string host, port;
    if (!File::ParseFullAddress(FLAGS_address, &host, &port, NULL)) {
        host = "";
        port = "";
    }
    if (host != "") {
        param["host"] = host;
    }
    if (port != "") {
        param["port"] = port;
    }
    if (FLAGS_user != "") {
        param["user"] = FLAGS_user;
    }
    if (FLAGS_password != "") {
        param["password"] = FLAGS_password;
    }
}

class ScannerTest : public testing::Test {
protected:
    virtual void SetUp() {
        ASSERT_TRUE(FLAGS_address != "");

        if (FLAGS_type == "local") {
            type = kLocalFs;
        } else if (FLAGS_type == "hdfs") {
            type = kInfHdfs;
        } else {
            ASSERT_TRUE(false);
        }

        if (FLAGS_format == "text") {
            format = kPlainText;
            scantype = kInputScanner;
        } else if (FLAGS_format == "seq") {
            format = kInfSeqFile;
            scantype = kInputScanner;
        } else if (FLAGS_format == "sort") {
            format = kInternalSortedFile;
            scantype = kInternalScanner;
        } else {
            ASSERT_TRUE(false);
        }
        FillParam(param);
    }

    virtual void TearDown() { }
};

TEST_F(ScannerTest, ScanningTest) {
    FormattedFile* fp = FormattedFile::Create(type, format, param);
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    EXPECT_EQ(fp->Error(), kOk);
    std::string key("key");
    std::string value("value");
    for (int i = 0; i < 100000; ++i) {
        std::stringstream ss;
        ss << std::setw(6) << std::setfill('0') << i;
        ASSERT_TRUE(fp->WriteRecord(key + ss.str(), value + ss.str() + '\n'));
        EXPECT_EQ(fp->Error(), kOk);
    }
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    Scanner* scanner = Scanner::Get(fp, scantype);
    ASSERT_TRUE(scanner != NULL);

    Scanner::Iterator* it = NULL;
    if (scantype == kInputScanner) {
        it = scanner->Scan(0, fp->GetSize());
        ASSERT_TRUE(it != NULL);
        EXPECT_EQ(it->Key(), "");
        EXPECT_EQ(it->Value(), "value000000");
    } else if (scantype == kInternalScanner) {
        it = scanner->Scan(Scanner::SCAN_KEY_BEGINNING, Scanner::SCAN_ALL_KEY);
        ASSERT_TRUE(it != NULL);
        EXPECT_EQ(it->Key(), "key000000");
        EXPECT_EQ(it->Value(), "value000000\n");
    }
    ASSERT_TRUE(it != NULL);
    EXPECT_TRUE(!it->Done());
    EXPECT_EQ(it->Error(), kOk);
    EXPECT_EQ(it->GetFileName(), scanner->GetFileName());

    int last_no = -1;
    for (; !it->Done(); it->Next()) {
        EXPECT_EQ(it->Error(), kOk);
        const std::string& key = it->Key();
        const std::string& value = it->Value();
        EXPECT_TRUE(key == "" || key.substr(0, 3) == "key");
        EXPECT_EQ(value.substr(0, 5), "value");
        // -1 indicates line-based format, so ignoring the key
        int cur_no_key = key == "" ? -1 : atoi(key.substr(3).c_str());
        int cur_no_value = atoi(value.substr(5).c_str());
        EXPECT_TRUE(cur_no_key == -1 || cur_no_value == cur_no_key);
        EXPECT_TRUE(last_no == -1 || cur_no_value == last_no + 1);
        last_no = cur_no_value;
    }
    EXPECT_EQ(it->Value(), it->Key() == "" ? "value099999" : "value099999\n");
    EXPECT_EQ(it->Error(), kNoMore);
    delete it;
    delete scanner;
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);
    delete fp;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}

