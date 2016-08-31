#include "common/file.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cstdlib>

using namespace baidu::shuttle;

/*
 * Notice: Test here will use a certain test location to validate the output
 *   The test directory should contain a sequence of integers named files starting from 1,
 *   containing n lines. Each filename is an integer, 1 bigger than the previous file
 */

DEFINE_string(type, "local", "set file type, local/hdfs is acceptable");
DEFINE_string(address, "", "full address to the test file");
DEFINE_string(empty_path, "", "path for remove test, must have same configs with address");
DEFINE_string(test_dir, "", "dir path as notice");
DEFINE_string(hdfs_user, "", "username to HDFS, empty means default");
DEFINE_string(hdfs_pass, "", "password to HDFS, empty only when username is empty");

// Global file pointer for FileIOTest, will be automatically initialized and released
static File* fp = NULL;

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

/*
 * This test checks the connectivity to the HDFS
 *   Need to manually check if the connectivity is same with the output
 *   Tests rely on ParseFullAddress
 */
TEST(FileToolsTest, ConnectInfHdfs) {
    File::Param param;
    FillParam(param);
    void* fs = NULL;
    ASSERT_TRUE(File::ConnectInfHdfs(param, &fs));
    ASSERT_TRUE(fs != NULL);
}

// BuildParam interface relies on ParseFullAddress
TEST(FileToolsTest, BuildParamTest) {
    DfsInfo info;
    const File::Param& param0 = File::BuildParam(info);
    EXPECT_TRUE(param0.find("host") == param0.end());
    EXPECT_TRUE(param0.find("port") == param0.end());
    EXPECT_TRUE(param0.find("user") == param0.end());
    EXPECT_TRUE(param0.find("password") == param0.end());

    info.set_host("localhost");
    info.set_port("9999");
    const File::Param& param1 = File::BuildParam(info);
    EXPECT_EQ(info.host(), "localhost");
    EXPECT_EQ(info.port(), "9999");
    EXPECT_TRUE(param1.find("host") != param1.end());
    EXPECT_EQ(param1.find("host")->second, "localhost");
    EXPECT_TRUE(param1.find("port") != param1.end());
    EXPECT_EQ(param1.find("port")->second, "9999");
    EXPECT_TRUE(param1.find("user") == param1.end());
    EXPECT_TRUE(param1.find("password") == param1.end());

    info.set_user("me");
    info.set_password("password");
    const File::Param& param2 = File::BuildParam(info);
    EXPECT_EQ(param2.find("host")->second, "localhost");
    EXPECT_EQ(param2.find("port")->second, "9999");
    EXPECT_EQ(info.user(), "me");
    EXPECT_EQ(info.password(), "password");
    EXPECT_TRUE(param2.find("user") != param2.end());
    EXPECT_EQ(param2.find("user")->second, "me");
    EXPECT_TRUE(param2.find("password") != param2.end());
    EXPECT_EQ(param2.find("password")->second, "password");

    info.set_path("hdfs://0.0.0.0:6666/whatever/file/is.file");
    const File::Param& param3 = File::BuildParam(info);
    EXPECT_EQ(info.host(), "0.0.0.0");
    EXPECT_EQ(info.port(), "6666");
    EXPECT_TRUE(param3.find("host") != param3.end());
    EXPECT_EQ(param3.find("host")->second, "0.0.0.0");
    EXPECT_TRUE(param3.find("port") != param3.end());
    EXPECT_EQ(param3.find("port")->second, "6666");
}

TEST(FileToolsTest, ParseAddressTest) {
    // --- HDFS format test ---
    std::string address = "hdfs://localhost:9999/home/test/hdfs.file";
    std::string host, port, path;
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "localhost");
    EXPECT_EQ(port, "9999");
    EXPECT_EQ(path, "/home/test/hdfs.file");

    address = "hdfs://0.0.0.0:/no/port/test.file";
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "0.0.0.0");
    EXPECT_EQ(port, "");
    EXPECT_EQ(path, "/no/port/test.file");

    address = "hdfs://:/empty/host/test.file";
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "");
    EXPECT_EQ(port, "");
    EXPECT_EQ(path, "/empty/host/test.file");

    address = "hdfs://localhost/no/colon/is/okay/test.file";
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "localhost");
    EXPECT_EQ(port, "");
    EXPECT_EQ(path, "/no/colon/is/okay/test.file");

    address = "hdfs:///no/host/port/info/test.file";
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "");
    EXPECT_EQ(port, "");
    EXPECT_EQ(path, "/no/host/port/info/test.file");

    // --- Local format test ---
    address = "file:///home/test/local.file";
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "");
    EXPECT_EQ(port, "");
    EXPECT_EQ(path, "/home/test/local.file");

    // Acceptable
    address = "file://localhost:80/local/with/host/test.file";
    EXPECT_TRUE(File::ParseFullAddress(address, &host, &port, &path));
    EXPECT_EQ(host, "localhost");
    EXPECT_EQ(port, "80");
    EXPECT_EQ(path, "/local/with/host/test.file");

    // --- Invalid format ---
    address = "dfs://localhost:9999/format/is/invalid/test.file";
    EXPECT_TRUE(!File::ParseFullAddress(address, &host, &port, &path));

    address = "";
    EXPECT_TRUE(!File::ParseFullAddress(address, &host, &port, &path));
}

TEST(FileToolsTest, PatternMatchTest) {
    // --- Perfect match test ---
    std::string pattern = "test_string";
    std::string origin = "test_string";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    // --- Star match test ---
    pattern = "*";
    origin = "whatever_the_string_is";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    pattern = "begin_*_end";
    origin = "begin_blahblahblah_end";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    // Check if * will be misled and teminated too soon
    pattern = ">*<";
    origin = ">mislead<test<";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    pattern = "/*/*/*";
    origin = "/multiple/star/match";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    pattern = "/*/*/*";
    origin = "//nothing/there";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    // --- Question mark match test ---
    pattern = "/aha?";
    origin = "/aha!";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    pattern = "/self/match?";
    origin = "/self/match?";
    EXPECT_TRUE(File::PatternMatch(origin, pattern));

    pattern = "/must/have/?/something";
    origin = "/must/have//something";
    EXPECT_TRUE(!File::PatternMatch(origin, pattern));
}

class FileIOTest : public testing::Test {
protected:
    virtual void SetUp() {
        if (fp != NULL) {
            return;
        }
        ASSERT_TRUE(FLAGS_address != "");
        FileType type = kLocalFs;
        if (FLAGS_type == "local") {
            type = kLocalFs;
        } else if (FLAGS_type == "hdfs") {
            type = kInfHdfs;
        } else {
            // Assert anyway
            ASSERT_EQ(FLAGS_type, "local");
        }
        File::Param param;
        FillParam(param);
        fp = File::Create(type, param);
        ASSERT_TRUE(fp != NULL);
    }

    virtual void TearDown() {
        if (fp != NULL) {
            delete fp;
        }
        fp = NULL;
    }
};

TEST_F(FileIOTest, OpenCloseNameTest) {
    EXPECT_EQ(fp->GetFileName(), "");

    File::Param param;
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    EXPECT_EQ(fp->GetFileName(), path);
    ASSERT_TRUE(fp->Close());

    // Check write-only open after file existence garanteed by operations above
    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    EXPECT_EQ(fp->GetFileName(), path);
    ASSERT_TRUE(fp->Close());
}

TEST_F(FileIOTest, ReadWriteTest) {
    File::Param param;
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));

    std::string write_buf;
    for (int i = 0; i < 100; ++i) {
        write_buf += "this is a test string\n";
    }
    ASSERT_TRUE(fp->Open(path, kWriteFile, param));
    ASSERT_TRUE(fp->WriteAll(write_buf.data(), write_buf.size()));
    ASSERT_TRUE(fp->Close());

    char* read_buf = new char[write_buf.size() + 1];
    ASSERT_TRUE(fp->Open(path, kReadFile, param));
    size_t read_n = fp->ReadAll(read_buf, write_buf.size() + 1);
    std::string read_str;
    read_str.assign(read_buf, read_n);
    EXPECT_EQ(write_buf, read_str);
    ASSERT_TRUE(fp->Close());
}

TEST_F(FileIOTest, TellSeekTest) {
    File::Param param;
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));
    ASSERT_TRUE(fp->Open(path, kReadFile, param));

    size_t size = fp->GetSize();
    EXPECT_TRUE(size != 0);

    EXPECT_EQ(fp->Tell(), 0);
    EXPECT_TRUE(fp->Seek(size >> 1));
    EXPECT_EQ(fp->Tell(), static_cast<int>(size >> 1));

    ASSERT_TRUE(fp->Close());
}

TEST_F(FileIOTest, RenameRemoveExistTest) {
    std::string path;
    ASSERT_TRUE(File::ParseFullAddress(FLAGS_address, NULL, NULL, &path));
    ASSERT_TRUE(fp->Exist(path));

    std::string new_path = path + "_new";
    EXPECT_TRUE(!fp->Exist(new_path));
    EXPECT_TRUE(fp->Rename(path, new_path));
    EXPECT_TRUE(!fp->Exist(path));
    EXPECT_TRUE(fp->Rename(new_path, path));
    EXPECT_TRUE(!fp->Exist(new_path));
    EXPECT_TRUE(fp->Exist(path));

    EXPECT_TRUE(fp->Exist(FLAGS_empty_path));
    EXPECT_TRUE(fp->Remove(FLAGS_empty_path));
    EXPECT_TRUE(!fp->Exist(FLAGS_empty_path));

    EXPECT_TRUE(fp->Mkdir(FLAGS_empty_path));
    EXPECT_TRUE(fp->Exist(FLAGS_empty_path));
}

struct FileInfoComparator {
    bool operator()(const FileInfo& lhs, const FileInfo& rhs) {
        // If no '/' is found, then find_last_of returns npos, add 1 will be 0
        std::string lns = lhs.name.substr(lhs.name.find_last_of('/') + 1);
        std::string rns = rhs.name.substr(rhs.name.find_last_of('/') + 1);
        return atoi(lns.c_str()) < atoi(rns.c_str());
    }
};

bool isFileInfoEqual(const FileInfo& lhs, const FileInfo& rhs) {
    return lhs.kind == rhs.kind && lhs.name == rhs.name && lhs.size ==rhs.size;
}

TEST_F(FileIOTest, ListGlobTest) {
    std::vector<FileInfo> list_children;
    std::string path = FLAGS_test_dir;

    EXPECT_TRUE(fp->List(path, &list_children));

    std::sort(list_children.begin(), list_children.end(), FileInfoComparator());
    int size = static_cast<int>(list_children.size());
    for (int i = 0; i < size; ++i) {
        const FileInfo& info = list_children[i];
        EXPECT_EQ(info.kind, 'F');
        std::string file_num = info.name.substr(info.name.find_last_of('/') + 1);
        EXPECT_EQ(atoi(file_num.c_str()), i + 1);
    }

    std::vector<FileInfo> glob_children;
    if (*path.rbegin() != '/') {
        path.push_back('/');
    }
    path.push_back('*');
    EXPECT_TRUE(fp->Glob(path, &glob_children));

    std::sort(glob_children.begin(), glob_children.end(), FileInfoComparator());
    EXPECT_TRUE(std::equal(list_children.begin(), list_children.end(),
            glob_children.begin(), isFileInfoEqual));
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

