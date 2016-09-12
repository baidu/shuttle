#include "common/fileformat.h"
#include "common/scanner.h"
#include "minion/input/merger.h"

#define STRIP_FLAG_HELP 1
#include <iostream>
#include <vector>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>
#include "common/file.h"
#include "logging.h"

DEFINE_string(u, "", "username to login dfs");
DEFINE_string(p, "", "password to login dfs");
DEFINE_string(s, " ", "separator to split record");
DEFINE_bool(v, false, "verbose mode to output everything");
DEFINE_bool(verbose, false, "verbose mode to output everything");
DEFINE_bool(i, false, "interactive mode");
DEFINE_bool(interact, false, "interactive mode");
DEFINE_bool(h, false, "show help info");
DECLARE_bool(help);

const std::string helper = "Tricorder - a tool to access internal sort file\n"
    "Usage: tricorder read/write [options] file...\n\n"
    "Options:\n"
    "  -u <username>     username to login dfs\n"
    "  -p <password>     password to login dfs\n"
    "  -s <separator>    use <separator> to split record and get key/value\n"
    "  -v, --verbose     output every information and operation status\n"
    "                    all output except for file access result will use stderr\n"
    "  -i, --interact    will provide a interactive interface\n"
    "                    better not use this when you need to use pipe\n"
    "  -h, --help        show this help info\n\n"
    "By default, tricorder use stdin to accept a pile of space-separated records\n"
    "in write operation, and output every k/v in sortfile in read operation\n"
    "Notice: file must be full address followed address schema\n";

static int InteractiveRead(baidu::shuttle::Scanner* scanner) {
    // scanner is non-null garanteed
    std::cout << "tricorder: interactive reader started" << std::endl
        << "  available operation: scan, printall" << std::endl;
    std::string operation;
    while (std::cout << "operation > " && std::getline(std::cin, operation)) {
        std::string start, end;
        if (operation == "scan") {
            std::cout << "start key > ";
            std::getline(std::cin, start);
            std::cout << "end key > ";
            std::getline(std::cin, end);
        } else if (operation == "printall") {
            start = baidu::shuttle::Scanner::SCAN_KEY_BEGINNING;
            end = baidu::shuttle::Scanner::SCAN_ALL_KEY;
        } else if (operation == "quit" || operation == "exit") {
            break;
        } else {
            std::cerr << "tricorder: no such operation" << std::endl;
            continue;
        }
        baidu::shuttle::Scanner::Iterator* it = scanner->Scan(start, end);
        if (it == NULL) {
            std::cerr << "tricorder: create scan iterator failed, sorry Bones" << std::endl;
            continue;
        }
        for (; !it->Done(); it->Next()) {
            std::cout << "key: " << it->Key() << ", value: " << it->Value() << std::endl;
        }
        if (it->Error() != baidu::shuttle::kOk && it->Error() != baidu::shuttle::kNoMore) {
            std::cerr << "[WARNING] iterate ends at " << it->GetFileName()
                << ", status: " << baidu::shuttle::Status_Name(it->Error()) << std::endl;
        }
        delete it;
    }
    std::cout << "tricorder: bye" << std::endl;
    return 0;
}

static int DirectRead(baidu::shuttle::Scanner* scanner) {
    // scanner is non-null garanteed
    baidu::shuttle::Scanner::Iterator* it = scanner->Scan(
            baidu::shuttle::Scanner::SCAN_KEY_BEGINNING,
            baidu::shuttle::Scanner::SCAN_ALL_KEY);
    if (it == NULL) {
        std::cerr << "tricorder: create scan iterator failed, sorry Bones" << std::endl;
        return -3;
    }
    for (; !it->Done(); it->Next()) {
        std::cout << "key: " << it->Key() << ", value: " << it->Value() << std::endl;
    }
    if (it->Error() != baidu::shuttle::kOk && it->Error() != baidu::shuttle::kNoMore) {
        std::cerr << "[WARNING] iterate ends at " << it->GetFileName()
            << ", status: " << baidu::shuttle::Status_Name(it->Error()) << std::endl;
        return -3;
    }
    delete it;
    return 0;
}

static int InteractiveWrite(baidu::shuttle::FormattedFile* fp) {
    // fp is non-null garanteed
    std::cout << "tricorder: interactive writer started" << std::endl
        << "  please offer sorted key/value data, use EOF to quit" << std::endl;
    const std::string& info = "please offer the record:";
    std::string key, value;
    while (std::cout << info << std::endl << "key > " && std::getline(std::cin, key)) {
        std::cout << "value > ";
        std::getline(std::cin, value);
        if (!fp->WriteRecord(key, value)) {
            std::cerr << "[WARNING] write record failed, key=" <<
                key << ", value=" << value << std::endl;
        } else {
            std::cout << "done" << std::endl;
        }
    }
    std::cout << "tricorder: bye" << std::endl;
    return 0;
}

static int DirectWrite(baidu::shuttle::FormattedFile* fp) {
    // fp is non-null garanteed
    std::string record, key, value;
    while (std::getline(std::cin, record)) {
        size_t separator = record.find_first_of(FLAGS_s);
        key = record.substr(0, separator);
        if (separator >= record.size() - 1) {
            value = "";
        } else {
            value = record.substr(separator + 1);
        }
        if (!fp->WriteRecord(key, value)) {
            std::cerr << "[WARNING] write record failed, key=" <<
                key << ", value=" << value << std::endl;
        }
    }
    return 0;
}

static int ReadFile(const std::vector<baidu::shuttle::FormattedFile*>& files) {
    baidu::shuttle::Scanner* scanner = NULL;
    if (files.size() == 1) {
        scanner = baidu::shuttle::Scanner::Get(files[0], baidu::shuttle::kInternalScanner);
    } else {
        scanner = new baidu::shuttle::Merger(files);
    }
    if (scanner == NULL) {
        std::cerr << "tricorder: create scanner failed, sorry Bones" << std::endl;
        return -3;
    }
    int ret = 0;
    if (FLAGS_i || FLAGS_interact) {
        ret = InteractiveRead(scanner);
    } else {
        ret = DirectRead(scanner);
    }
    delete scanner;
    return ret;
}

static int WriteFile(baidu::shuttle::FormattedFile* fp) {
    // fp is non-null garanteed
    int ret = 0;
    if (FLAGS_i || FLAGS_interact) {
        ret = InteractiveWrite(fp);
    } else {
        ret = DirectWrite(fp);
    }
    return ret;
}

// Declare log file pointer to redirect them later
namespace baidu {
namespace common {
extern FILE* g_log_file;
extern FILE* g_warning_file;
}
}

int main(int argc, char** argv) {
    google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
    if (FLAGS_h || FLAGS_help) {
        std::cerr << helper;
        return 1;
    }
    if (argc < 2) {
        std::cerr << "tricorder: no operation specified, poweroff" << std::endl;
        return -1;
    }
    // Prepare file names
    std::vector<std::string> filenames;
    std::string operation(argv[1]);
    if (operation == "read") {
        if (argc < 3) {
            std::cerr << "tricorder: read operation needs at least one resource file"
                << std::endl;
            return -1;
        }
        for (int i = 2; i < argc; ++i) {
            filenames.push_back(argv[i]);
        }
    } else if (operation == "write") {
        if (argc != 3) {
            std::cerr << "tricorder: write operation only accepts one destination"
                << std::endl;
            return -1;
        }
        filenames.push_back(argv[2]);
    } else {
        std::cerr << "tricorder: unfamiliar operation: " << operation << std::endl;
        return -2;
    }
    // Set log level for verbose mode
    bool verbose = FLAGS_v || FLAGS_verbose;
    if (verbose) {
        baidu::common::SetLogLevel(baidu::common::DEBUG);
    } else {
        baidu::common::SetLogLevel(baidu::common::WARNING);
    }
    // Redirect log output to separate file access results and information
    baidu::common::g_log_file = stderr;
    baidu::common::g_warning_file = stderr;
    std::vector<baidu::shuttle::FormattedFile*> files;
    for (std::vector<std::string>::iterator it = filenames.begin();
            it != filenames.end(); ++it) {
        const std::string& name = *it;
        std::string host, port, path;
        if (!baidu::shuttle::File::ParseFullAddress(name, &host, &port, &path)) {
            std::cerr << "[WARNING] ignore invalid full address: " << name << std::endl;
            continue;
        }
        baidu::shuttle::File::Param param;
        if (!host.empty()) {
            param["host"] = host;
        }
        if (!port.empty()) {
            param["port"] = port;
        }
        if (!FLAGS_u.empty()) {
            param["user"] = FLAGS_u;
        }
        if (!FLAGS_p.empty()) {
            param["password"] = FLAGS_p;
        }
        baidu::shuttle::FileType type;
        // Valid file type garanteed
        if (boost::starts_with(name, "hdfs://")) {
            type = baidu::shuttle::kInfHdfs;
        } else {
            type = baidu::shuttle::kLocalFs;
        }
        baidu::shuttle::FormattedFile* fp =
            baidu::shuttle::FormattedFile::Create(type, baidu::shuttle::kInternalSortedFile, param);
        if (fp == NULL) {
            std::cerr << "[WARNING] ignore null file pointer: " << name << std::endl;
            continue;
        }
        baidu::shuttle::OpenMode mode = baidu::shuttle::kReadFile;
        if (operation == "write") {
            mode = baidu::shuttle::kWriteFile;
        }
        if (!fp->Open(path, mode, param)) {
            std::cerr << "[WARNING] fail to open file, ignore: " << name << std::endl;
            continue;
        }
        files.push_back(fp);
    }
    if (files.empty()) {
        std::cerr << "tricorder: all provided files are not available, sorry Bones" << std::endl;
        return -3;
    }
    int ret = 0;
    // Valid operation garanteed
    if (operation == "read") {
        ret = ReadFile(files);
    } else {
        ret = WriteFile(files[0]);
    }
    // Cleaning up
    for (std::vector<baidu::shuttle::FormattedFile*>::iterator it = files.begin();
            it != files.end(); ++it) {
        (*it)->Close();
        delete (*it);
    }
    return ret;
}

