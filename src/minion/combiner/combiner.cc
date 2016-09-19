#include "combiner.h"

#include <boost/bind.hpp>
#include <algorithm>
#include <unistd.h>
#include <sys/wait.h>
#include <cstdlib>
#include "thread.h"
#include "logging.h"

namespace baidu {
namespace shuttle {

struct CombinerItemLess {
    bool operator()(EmitItem* const& a, EmitItem* const& b) {
        // Type cast here is safety since this comparator only used in Combiner
        return static_cast<CombinerItem*>(a)->key < static_cast<CombinerItem*>(b)->key;
    }
};

// Flushing in Combiner means invoking user command and dealing with its output
Status Combiner::Flush() {
    if (mem_table_.empty()) {
        return kNoMore;
    }
    int child_pid = -1;
    int stdin_fno = -1;
    int stdout_fno = -1;
    // Fork a new process to run user command
    if (!InvokeUserCommand(&child_pid, &stdin_fno, &stdout_fno)) {
        return kUnKnown;
    }
    // Create a new thread to write to user command
    ::baidu::common::Thread write_thread;
    write_thread.Start(boost::bind(&Combiner::FlushDataToUser, this, stdout_fno));
    // Read from user command and send to stdout
    if (!WriteUserOutput(stdin_fno)) {
        return kWriteFileFail;
    }
    // Wait until user program exit
    int exit_code = 0;
    ::waitpid(child_pid, &exit_code, 0);
    LOG(INFO, "child process exit with code: %d", exit_code);
    write_thread.Join();
    Reset();
    if (exit_code != 0) {
        return kUnKnown;
    }
    return kOk;
}

bool Combiner::InvokeUserCommand(int* child, int* in_fd, int* out_fd) {
    int stdin_pipes[2];
    int stdout_pipes[2];
    /*
     * Open a pipe for communication
     *   stdin_pipes: [0] as child's stdin, [1] as parent's output
     *   stdout_pipes: [0] as parent's input, [1] as child's stdout
     */
    ::pipe(stdin_pipes);
    ::pipe(stdout_pipes);
    pid_t child_pid = ::fork();
    if (child_pid == -1) {
        LOG(WARNING, "failed to fork a child process");
        exit(1);
    } else if (child_pid == 0) {
        ::close(stdin_pipes[1]);
        ::close(stdout_pipes[0]);
        // Duplicate pipe fd to standard io fd
        ::dup2(stdin_pipes[0], STDIN_FILENO);
        ::dup2(stdout_pipes[1], STDOUT_FILENO);
        char* cmd_argv[] = { (char*)"sh", (char*)"-c", (char*)user_cmd_.c_str(), NULL };
        char* env[] = { NULL };
        ::execve("/bin/sh", cmd_argv, env);
        /*
         * Exec a new program will ignore the rest of the code below,
         *   so it can never get to the following assertion
         */
        assert(0);
    }
    close(stdin_pipes[0]);
    close(stdout_pipes[1]);
    if (child_pid != 0) {
        *child = child_pid;
    }
    if (in_fd != NULL) {
        *in_fd = stdout_pipes[0];
    }
    if (out_fd != NULL) {
        *out_fd = stdin_pipes[1];
    }
    return true;
}

bool Combiner::WriteUserOutput(int in_fd) {
    char buf[4096];
    FILE* user_output = ::fdopen(in_fd, "r");
    if (user_output == NULL) {
        return false;
    }
    while (!feof(user_output)) {
        int n = fread(buf, 1, sizeof(buf), user_output);
        if (n <= 0) {
            break;
        }
        if (fwrite(buf, 1, n, stdout) != static_cast<size_t>(n)) {
            // fail to write to stdout means unrecoverable situation
            return false;
        }
    }
    fflush(stdout);
    fclose(user_output);
    return true;
}

void Combiner::FlushDataToUser(int out_fd) {
    std::sort(mem_table_.begin(), mem_table_.end(), CombinerItemLess());
    FILE* to_user = ::fdopen(out_fd, "w");
    for (std::vector<EmitItem*>::iterator it = mem_table_.begin();
            it != mem_table_.end(); ++it) {
        CombinerItem* cur = static_cast<CombinerItem*>(*it);
        fwrite(cur->record.data(), 1, cur->record.size(), to_user);
    }
    fflush(to_user);
    fclose(to_user);
}

}
}

