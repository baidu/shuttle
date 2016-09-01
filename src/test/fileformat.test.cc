#include "common/fileformat.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <sstream>
#include <cstdlib>

using namespace baidu::shuttle;

/*
 * Test need an address to an inexist location, and will automatically create testcase file
 * But this test is not responsible for destroying test file and need manual operation
 * FormattedFile doesn't provide delete interface so it needs user to delete it
 */

DEFINE_string(type, "local", "set file type, local/hdfs is acceptable");
DEFINE_string(format, "text", "set file format, text/seq/sort is acceptable");
DEFINE_string(address, "", "full address to the test file");
DEFINE_string(user, "", "username to FS, empty means default");
DEFINE_string(password, "", "password to FS, empty only when username is empty");

// Global file pointer for testing, will be automatically initialized and released
static FormattedFile* fp = NULL;
// Path information in address. Initialized when FormattedFileTest environments set up
static std::string path;
static bool isInputFile = false;
static bool isInternalFile = false;

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

class FormattedFileTest : public testing::Test {
protected:
    virtual void SetUp() {
        if (fp != NULL) {
            return;
        }
        ASSERT_TRUE(FLAGS_address != "");
        ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

        FileType type = kLocalFs;
        FileFormat format = kPlainText;
        if (FLAGS_type == "local") {
            type = kLocalFs;
        } else if (FLAGS_type == "hdfs") {
            type = kInfHdfs;
        } else {
            ASSERT_TRUE(false);
        }

        if (FLAGS_format == "text") {
            format = kPlainText;
            isInputFile = true;
            isInternalFile = false;
        } else if (FLAGS_format == "seq") {
            format = kInfSeqFile;
            isInputFile = true;
            isInternalFile = false;
        } else if (FLAGS_format == "sort") {
            format = kInternalSortedFile;
            isInputFile = false;
            isInternalFile = true;
        } else {
            ASSERT_TRUE(false);
        }

        File::Param param;
        FillParam(param);
        fp = FormattedFile::Create(type, format, param);
        ASSERT_TRUE(fp != NULL);
    }

    virtual void TearDown() {
        if (fp != NULL) {
            delete fp;
        }
        fp = NULL;
    }
};

TEST_F(FormattedFileTest, OpenCloseTest) {
    File::Param param;
    // Create or truncate file
    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    EXPECT_EQ(fp->Error(), kOk);
    EXPECT_EQ(fp->GetFileName(), path);
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    EXPECT_EQ(fp->Error(), kOk);
    EXPECT_EQ(fp->GetFileName(), path);
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);
}

TEST_F(FormattedFileTest, ReadWriteTest) {
    File::Param param;
    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    EXPECT_EQ(fp->Error(), kOk);
    std::string key("key");
    std::string value("value");
    for (int i = 0; i < 100000; ++i) {
        std::stringstream ss;
        ss << std::setw(6) << std::setfill('0') << i;
        // Add endline to be friendly with line-based format
        ASSERT_TRUE(fp->WriteRecord(key + ss.str(), value + ss.str() + '\n'));
        EXPECT_EQ(fp->Error(), kOk);
    }
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    ASSERT_TRUE(fp->Locate("key000000"));
    EXPECT_EQ(fp->Error(), kOk);
    key = "";
    for (int i = 0; i < 100000; ++i) {
        ASSERT_TRUE(fp->ReadRecord(key, value));
        EXPECT_EQ(fp->Error(), kOk);
        EXPECT_TRUE(key == "" || atoi(key.substr(3).c_str()) == i);
        EXPECT_EQ(atoi(value.substr(5).c_str()), i);
    }
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);
}

TEST_F(FormattedFileTest, LocationChangeTest) {
    // Prepare test file
    File::Param param;
    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    EXPECT_EQ(fp->Error(), kOk);
    std::string key("key");
    std::string value("value");
    for (int i = 0; i < 100000; ++i) {
        std::stringstream ss;
        ss << std::setw(6) << std::setfill('0') << i;
        // Add endline to be friendly with line-based format
        ASSERT_TRUE(fp->WriteRecord(key + ss.str(), value + ss.str() + '\n'));
        EXPECT_EQ(fp->Error(), kOk);
    }
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    EXPECT_EQ(fp->Error(), kOk);

    if (isInputFile) {
        EXPECT_EQ(fp->Tell(), 0);
        EXPECT_EQ(fp->Error(), kOk);
        int64_t size = fp->GetSize();
        ASSERT_TRUE(size > 0);
        EXPECT_TRUE(fp->Seek(size >> 1));
        EXPECT_EQ(fp->Error(), kOk);
        EXPECT_TRUE(fp->Tell() >= (size >> 1));
        EXPECT_EQ(fp->Error(), kOk);
    }

    if (isInternalFile) {
        ASSERT_TRUE(fp->Locate("key050000"));
        EXPECT_EQ(fp->Error(), kOk);
        EXPECT_TRUE(fp->ReadRecord(key, value));
        EXPECT_EQ(fp->Error(), kOk);
        EXPECT_EQ(key, "key050000");
        EXPECT_EQ(value, "value050000\n");
    }

    key = "";
    int last_no = -1;
    // Check the continuity of the records
    while (fp->ReadRecord(key, value)) {
        EXPECT_EQ(fp->Error(), kOk);
        EXPECT_TRUE(key == "" || key.substr(0, 3) == "key");
        EXPECT_EQ(value.substr(0, 5), "value");
        // -1 indicates line-based format, so ignoring the key
        int cur_no_key = key == "" ? -1 : atoi(key.substr(3).c_str());
        int cur_no_value = atoi(value.substr(5).c_str());
        EXPECT_TRUE(cur_no_key == -1 || cur_no_value == cur_no_key);
        EXPECT_TRUE(last_no == -1 || cur_no_value == last_no + 1);
        last_no = cur_no_value;
    }
    EXPECT_EQ(value, "value099999\n");
    EXPECT_EQ(fp->Error(), kNoMore);
    ASSERT_TRUE(fp->Close());
    EXPECT_EQ(fp->Error(), kOk);
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

