#include <gtest/gtest.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include "input_reader.h"

using namespace baidu::shuttle;

int g_total_line = 100000;
int g_total_bytes = 0;
const char* g_file_name = "/tmp/file_input.txt";

TEST(InputReader, Prepare) {
    FILE* file = fopen(g_file_name, "w");
    srand(time(NULL));
    for (int i = 0; i < g_total_line; i++) {
        char buf[256];
        snprintf(buf, sizeof(buf), "key_%d:", i);
        std::string line(buf);
        int line_nbytes = rand() % 1000;
        line.append(line_nbytes, 'x');
        fprintf(file, "%s\n", line.c_str());
        g_total_bytes += (line.size() + 1);
    }
    fclose(file);
}

TEST(InputReader, Split1) {
    InputReader * reader = InputReader::CreateLocalTextReader();
    FileSystem::Param param;
    Status status = reader->Open(g_file_name, param);
    EXPECT_EQ(status, kOk);
    int block_size = 12580;
    int start_offset = 0;
    int line_count = 0;
    int bytes_count = 0;
    while (start_offset < g_total_bytes) {
        InputReader::Iterator* it = reader->Read(start_offset, block_size);
        while (!it->Done()) {
            line_count++;
            bytes_count += (it->Line().size() + 1);
            it->Next();
        }
        EXPECT_EQ(it->Error(), kNoMore);
        delete it;
        start_offset += block_size;
    }
    EXPECT_EQ(line_count, g_total_line);
    EXPECT_EQ(g_total_bytes, bytes_count);
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    delete reader;
}

TEST(InputReader, Split2) {
    InputReader * reader = InputReader::CreateLocalTextReader();
    FileSystem::Param param;
    Status status = reader->Open(g_file_name, param);
    EXPECT_EQ(status, kOk);
    int block_size = 45678;
    int start_offset = 0;
    int line_count = 0;
    int bytes_count = 0;
    while (start_offset < g_total_bytes) {
        InputReader::Iterator* it = reader->Read(start_offset, block_size);
        while (!it->Done()) {
            line_count++;
            bytes_count += (it->Line().size() + 1);
            it->Next();
        }
        EXPECT_EQ(it->Error(), kNoMore);
        delete it;
        start_offset += block_size;
    }
    EXPECT_EQ(line_count, g_total_line);
    EXPECT_EQ(g_total_bytes, bytes_count);
    status = reader->Close();
    EXPECT_EQ(status, kOk);
    delete reader;
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
