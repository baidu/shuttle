#include "common/fileformat.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <boost/lexical_cast.hpp>
#include <sstream>

using namespace baidu::shuttle;

/*
 * Notice: Test here will use a certain test file to validate the output
 *   The test file should be a sequence of integers starting from 0, containing n lines
 *   Each line is composed with an integer, 1 bigger than the previous line
 */

DEFINE_string(type, "local", "set file type, local/hdfs is acceptable");
DEFINE_string(format, "text", "set file format. text/seq/sort is acceptable");
DEFINE_string(address, "", "full address to the test file");
DEFINE_string(hdfs_user, "", "username to HDFS, empty means default");
DEFINE_string(hdfs_pass, "", "password to HDFS, empty only when username is empty");

// Global file pointer for testing, will be automatically initialized and released
FormattedFile* fp = NULL;
bool isInputFile = false;
bool isInternalFile = false;

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
    if (FLAGS_hdfs_user != "") {
        param["user"] = FLAGS_hdfs_user;
    }
    if (FLAGS_hdfs_pass != "") {
        param["password"] = FLAGS_hdfs_pass;
    }
}

class FormattedFileTest : public testing::Test {
protected:
    virtual void SetUp() {
        if (fp != NULL) {
            return;
        }
        ASSERT_TRUE(FLAGS_address != "");
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

TEST_F(FormattedFileTest, OpenCloseForReadTest) {
    File::Param param;
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    EXPECT_EQ(fp->GetFileName(), path);
    ASSERT_TRUE(fp->Close());
}

TEST_F(FormattedFileTest, ReadWriteTest) {
    File::Param param;
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    std::string key("key");
    std::string value("value");
    for (int i = 0; i < 100000; ++i) {
        std::stringstream ss;
        ss << std::setw(6) << std::setfill('0') << i << std::endl;
        EXPECT_TRUE(fp->WriteRecord(key + ss.str(), value + ss.str()));
    }
    ASSERT_TRUE(fp->Close());

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    key = "";
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(fp->ReadRecord(key, value));
        EXPECT_TRUE(key == "" || boost::lexical_cast<int>(key.substr(3).c_str()) == i);
        EXPECT_EQ(boost::lexical_cast<int>(value.substr(5).c_str()), i);
    }
    ASSERT_TRUE(fp->Close());
}

// LocationChangeTest relies on the result of ReadWriteTest
TEST_F(FormattedFileTest, LocationChangeTest) {
    File::Param param;
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

    if (isInputFile) {
        ASSERT_TRUE(fp->Open(path, kReadFile, param));
        EXPECT_EQ(fp->Tell(), 0);
        EXPECT_TRUE(fp->Seek(100));
        EXPECT_EQ(fp->Tell(), 100);
        std::string key, value;
        EXPECT_TRUE(fp->ReadRecord(key, value));
        ASSERT_TRUE(fp->Close());
    }
    if (isInternalFile) {
        ASSERT_TRUE(fp->Open(path, kReadFile, param));
        EXPECT_TRUE(fp->Locate("key000011"));
        std::string key, value;
        EXPECT_TRUE(fp->ReadRecord(key, value));
        EXPECT_EQ(key, "key000011");
        EXPECT_EQ(value, "value000011");
        ASSERT_TRUE(fp->Close());
    }
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

