#define _GNU_SOURCE
#include "core.h"
#include "jail.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static unsigned int checks;
#define CHECK(expression) do { ++checks; if (!(expression)) { \
    fprintf(stderr, "Selftest failed at line %d: %s\n", __LINE__, #expression); exit(1); } } while (0)

struct time_source { uint64_t now; };
static uint64_t fake_clock(void *context) { return ((struct time_source *) context)->now; }
static char temporary[128] = "/tmp/checkpoint-fuse-selftest-XXXXXX";
static unsigned int journal_number;

static int new_state(struct cf_state *state, struct time_source *clock, uint64_t records, char path[PATH_MAX]) {
    snprintf(path, PATH_MAX, "%s/journal-%u.jsonl", temporary, ++journal_number);
    int fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    CHECK(fd >= 0);
    CHECK(cf_init(state, "attempt", "storage", "instance", "job", fd, 8 * 1024 * 1024, records,
                  fake_clock, fake_clock, clock) == 0);
    CHECK(state->mode == CF_STARTING);
    CHECK(cf_ready(state) == 0);
    CHECK(state->mode == CF_HEALTHY);
    return fd;
}

static int control(struct cf_state *state, const char *line, const char *expected) {
    char response[CF_RESPONSE_MAX];
    int result = cf_control(state, line, response, sizeof(response));
    CHECK(strstr(response, expected) != NULL);
    CHECK(response[strlen(response) - 1] == '\n');
    return result;
}

static int guarded_write(struct cf_state *state, int fd, const char *value) {
    pthread_mutex_lock(&state->mutex);
    int result = cf_begin_locked(state, "write");
    if (!result) { ssize_t count = pwrite(fd, value, strlen(value), 0); result = count < 0 ? -errno : (int) count; }
    CHECK(cf_record_locked(state, "write", "write", "job/state.bin", result < 0 ? -1 : result,
                           result < 0 ? -result : 0, strlen(value), result > 0 ? (uint64_t) result : 0,
                           -1, result < 0 ? "eio-rule" : "success") == 0);
    pthread_mutex_unlock(&state->mutex);
    return result;
}

static void control_and_held_descriptor(void) {
    struct cf_state state; struct time_source clock = {.now = UINT64_C(1000000000)};
    char journal_path[PATH_MAX], data_path[PATH_MAX];
    int journal = new_state(&state, &clock, 10000, journal_path);
    snprintf(data_path, sizeof(data_path), "%s/held.bin", temporary);
    int held = open(data_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    CHECK(held >= 0);
    CHECK(guarded_write(&state, held, "original") == 8);
    CHECK(control(&state, "STATUS attempt storage instance 0", "\"state\":\"HEALTHY\"") == 0);
    CHECK(control(&state, "ARM attempt storage other 1 fault 500 create,write", "foreign-identity") < 0);
    CHECK(state.mode == CF_HEALTHY && state.revision == 0);
    CHECK(control(&state, "ARM attempt storage instance 01 fault 500 create,write", "malformed-command") < 0);
    CHECK(control(&state, "ARM attempt storage instance 1 fault 120001 create,write", "invalid-arm") < 0);
    CHECK(control(&state, "ARM attempt storage instance 1 fault 500 write", "invalid-arm") < 0);
    CHECK(control(&state, "ARM attempt storage instance 1 fault 500 create,write", "\"state\":\"ARMED\"") == 0);
    CHECK(state.revision == 1 && state.lease_ms == 500 && state.deadline_ns == UINT64_C(1500000000));
    CHECK(guarded_write(&state, held, "corrupt!") == -EIO);
    char bytes[9] = {0}; CHECK(pread(held, bytes, 8, 0) == 8); CHECK(!strcmp(bytes, "original"));
    pthread_mutex_lock(&state.mutex);
    CHECK(cf_begin_locked(&state, "create") == -EIO);
    CHECK(cf_begin_locked(&state, NULL) == 0);
    pthread_mutex_unlock(&state.mutex);
    CHECK(control(&state, "ARM attempt storage instance 1 fault 500 create,write", "stale-revision") < 0);
    CHECK(control(&state, "HEAL attempt storage instance 2 different", "fault-id-mismatch") < 0);
    CHECK(control(&state, "HEAL attempt storage instance 2 fault", "\"state\":\"HEALTHY\"") == 0);
    CHECK(guarded_write(&state, held, "restored") == 8);
    CHECK(pread(held, bytes, 8, 0) == 8 && !strcmp(bytes, "restored"));
    CHECK(control(&state, "ARM attempt storage instance 3 expiry 500 create,write", "\"state\":\"ARMED\"") == 0);
    clock.now = UINT64_C(1500000000);
    cf_watchdog(&state);
    CHECK(state.mode == CF_EXPIRED);
    CHECK(guarded_write(&state, held, "corrupt!") == -EIO);
    CHECK(control(&state, "HEAL attempt storage instance 4 expiry", "terminal-state") < 0);
    CHECK(control(&state, "STOP attempt storage instance 4", "\"state\":\"EXPIRED\"") == 0);
    CHECK(state.stop_requested && state.journal_complete);
    char journal_text[65536]; ssize_t size = pread(journal, journal_text, sizeof(journal_text) - 1, 0);
    CHECK(size > 0); journal_text[size] = '\0';
    CHECK(strstr(journal_text, "\"operation\":\"EXPIRE\"") != NULL);
    CHECK(strstr(journal_text, "\"errno\":5") != NULL);
    CHECK(strstr(journal_text, "\"allowedOperations\":[\"create\",\"write\"]") != NULL);
    CHECK(strstr(journal_text, "\"leaseMs\":500") != NULL);
    CHECK(strstr(journal_text, "direct_io=1,writeback_cache=0,entry_timeout=0,attr_timeout=0,negative_timeout=0,kernel_cache=0,auto_cache=0") != NULL);
    close(held); close(journal); cf_destroy(&state); CHECK(unlink(data_path) == 0); CHECK(unlink(journal_path) == 0);
}

static void abort_and_bounds(void) {
    struct cf_state state; struct time_source clock = {.now = UINT64_C(1000000000)};
    char path[PATH_MAX]; int fd = new_state(&state, &clock, 10000, path);
    CHECK(control(&state, "ARM attempt storage instance 1 canceled 1000 create,write", "\"ok\":true") == 0);
    CHECK(control(&state, "ABORT attempt storage instance 2 canceled", "\"state\":\"ABORTED\"") == 0);
    CHECK(control(&state, "HEAL attempt storage instance 3 canceled", "terminal-state") < 0);
    pthread_mutex_lock(&state.mutex); CHECK(cf_begin_locked(&state, "write") == -EIO); CHECK(cf_begin_locked(&state, NULL) == 0); pthread_mutex_unlock(&state.mutex);
    CHECK(control(&state, "STOP attempt storage instance 3", "\"stopping\":true") == 0);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);

    fd = new_state(&state, &clock, 10, path);
    uint64_t initial_sequence = state.sequence;
    for (unsigned int index = 0; index < 12; ++index) {
        char response[CF_RESPONSE_MAX]; (void) cf_control(&state, "STATUS attempt storage instance 0", response, sizeof(response));
    }
    CHECK(state.mode == CF_HEALTHY && state.journal_complete && state.sequence == initial_sequence);
    pthread_mutex_lock(&state.mutex);
    for (unsigned int index = 0; index < 12; ++index)
        (void) cf_record_locked(&state, "write", "write", "job/file", 1, 0, 1, 1, -1, "success");
    CHECK(cf_begin_locked(&state, NULL) == 0);
    CHECK(cf_begin_locked(&state, "write") == -EIO);
    CHECK(cf_begin_operation_locked(&state, NULL, true) == -EIO);
    CHECK(cf_record_locked(&state, "getattr", NULL, "job/file", 0, 0, 0, 0, -1, "success") == 0);
    pthread_mutex_unlock(&state.mutex);
    CHECK(state.mode == CF_FATAL && !state.journal_complete && state.sequence <= 10);
    CHECK(control(&state, "STATUS attempt storage instance 0", "\"journalComplete\":false") == 0);
    CHECK(control(&state, "ARM attempt storage instance 1 forbidden 10 create,write", "\"ok\":false") < 0);
    CHECK(control(&state, "STOP attempt storage instance 1", "\"state\":\"FATAL\"") == 0);
    char text[16384]; ssize_t size = pread(fd, text, sizeof(text) - 1, 0); CHECK(size > 0); text[size] = '\0';
    CHECK(strstr(text, "journal-bound-exceeded") != NULL);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);

    /* A callback that emits two receipts must still preserve the terminal slot. */
    fd = new_state(&state, &clock, 10, path);
    pthread_mutex_lock(&state.mutex);
    while (state.sequence < 8)
        CHECK(cf_record_locked(&state, "write", "write", "job/file", 0, 0, 0, 0, -1, "success") == 0);
    CHECK(cf_begin_locked(&state, NULL) == 0);
    CHECK(cf_record_locked(&state, "rename-target", NULL, "job/new", -1, EPERM, 0, 0, -1, "backing-or-path-error") == 0);
    CHECK(cf_record_locked(&state, "rename", NULL, "job/old", -1, EPERM, 0, 0, -1, "backing-or-path-error") == -EIO);
    CHECK(state.mode == CF_FATAL && !state.journal_complete && state.sequence == 10);
    pthread_mutex_unlock(&state.mutex);
    size = pread(fd, text, sizeof(text) - 1, 0); CHECK(size > 0); text[size] = '\0';
    CHECK(strstr(text, "\"operation\":\"FATAL\"") != NULL);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);

    fd = new_state(&state, &clock, 10000, path);
    CHECK(close(fd) == 0);
    CHECK(control(&state, "ARM attempt storage instance 1 fault 500 create,write", "journal-incomplete") < 0);
    CHECK(state.mode == CF_FATAL && !state.journal_complete);
    cf_destroy(&state); CHECK(unlink(path) == 0);

    fd = new_state(&state, &clock, 10000, path);
    char too_long[CF_PATH_MAX + 2]; memset(too_long, 'x', sizeof(too_long) - 1); too_long[sizeof(too_long) - 1] = '\0';
    pthread_mutex_lock(&state.mutex);
    CHECK(cf_record_locked(&state, "getattr", NULL, too_long, -1, ENAMETOOLONG, 0, 0, -1, "invalid-path") == 0);
    CHECK(state.mode == CF_HEALTHY && state.journal_complete);
    pthread_mutex_unlock(&state.mutex);
    size = pread(fd, text, sizeof(text) - 1, 0); CHECK(size > 0); text[size] = '\0';
    CHECK(strstr(text, "\"pathTruncated\":true") != NULL);
    CHECK(strstr(text, "receipt-path-truncated") != NULL);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);

    fd = new_state(&state, &clock, 10000, path);
    CHECK(control(&state, "STOP attempt storage instance 1", "\"state\":\"STOPPED\"") == 0);
    cf_shutdown(&state, "loop-ended-without-stop"); CHECK(state.mode == CF_STOPPED && state.journal_complete);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);
}

static void aggregate_reads_and_durability(void) {
    struct cf_state state; struct time_source clock = {.now = UINT64_C(1000000000)};
    char path[PATH_MAX]; int fd = new_state(&state, &clock, 10000, path);
    pthread_mutex_lock(&state.mutex);
    for (unsigned int i = 0; i < 100000; ++i)
        CHECK(cf_record_locked(&state, "getattr", NULL, "job/file", 0, 0, 0, 0, -1, "success") == 0);
    CHECK(state.sequence == 102 && state.counts[1] == 100000);
    uint64_t before = state.sequence;
    CHECK(cf_record_locked(&state, "read", NULL, "job/ordinary", 8, 0, 8, 8, -1, "success") == 0);
    CHECK(state.sequence == before && state.read_bytes == 8);
    CHECK(cf_record_locked(&state, "read", NULL, "job/id/chk-7/state", 8, 0, 8, 8, -1, "success") == 0);
    CHECK(state.sequence == before + 1 && state.read_bytes == 16);
    CHECK(cf_record_locked(&state, "getattr", NULL, "job/missing", -1, ENOENT, 0, 0, -1, "backing-or-path-error") == 0);
    CHECK(state.sequence == before + 2 && state.counts[1] == 100000);
    pthread_mutex_unlock(&state.mutex);
    clock.now += CF_SUMMARY_NS;
    cf_watchdog(&state); CHECK(state.sequence == before + 3);
    before = state.sequence;
    clock.now += CF_SUMMARY_NS; cf_watchdog(&state); CHECK(state.sequence == before);
    CHECK(control(&state, "STATUS attempt storage instance 0", "\"getattr\":{\"count\":100000,\"bytes\":0}") == 0);
    CHECK(state.sequence == before && state.journal_complete);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);

    /* A pipe rejects fdatasync. SUMMARY remains a normal append, while a fault-family
       receipt fails closed and consumes its sequence before the terminal receipt. */
    fd = new_state(&state, &clock, 10000, path);
    int pipes[2]; CHECK(pipe(pipes) == 0); state.journal_fd = pipes[1];
    pthread_mutex_lock(&state.mutex);
    CHECK(cf_record_locked(&state, "getattr", NULL, "job/file", 0, 0, 0, 0, -1, "success") == 0);
    pthread_mutex_unlock(&state.mutex);
    clock.now += CF_SUMMARY_NS; cf_watchdog(&state);
    CHECK(state.mode == CF_HEALTHY && state.sequence == 3);
    pthread_mutex_lock(&state.mutex);
    CHECK(cf_record_locked(&state, "write", "write", "job/file", 1, 0, 1, 1, -1, "success") == -EIO);
    CHECK(state.mode == CF_FATAL && !state.journal_complete && state.sequence == 5);
    pthread_mutex_unlock(&state.mutex);
    close(pipes[1]); char text[8192]; ssize_t size = read(pipes[0], text, sizeof(text) - 1);
    CHECK(size > 0); text[size] = '\0';
    CHECK(strstr(text, "\"sequence\":3,") != NULL && strstr(text, "\"sequence\":4,") != NULL && strstr(text, "\"sequence\":5,") != NULL);
    close(pipes[0]); close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);
}

static void strict_receipts_and_sync_counters(void) {
    struct cf_state state; struct time_source clock = {.now = UINT64_C(1000000000)};
    char path[PATH_MAX]; int fd = new_state(&state, &clock, 10000, path);
    const char *unrecorded[] = {"open", "opendir", "fsyncdir", "mkdir", "rmdir", "unlink",
                                "rename", "rename-target", "chmod", "chown", "utimens", "statfs"};
    pthread_mutex_lock(&state.mutex);
    uint64_t before = state.sequence;
    for (size_t index = 0; index < sizeof(unrecorded) / sizeof(unrecorded[0]); ++index) {
        CHECK(cf_record_locked(&state, unrecorded[index], NULL, "job/id/chk-7/file", 0, 0, 0, 0, -1, "success") == 0);
        CHECK(state.sequence == before);
    }
    CHECK(cf_record_locked(&state, "fsync", NULL, "job/file", 0, 0, 0, 0, 0, "success") == 0);
    CHECK(cf_record_locked(&state, "fsync", NULL, "job/file", 0, 0, 0, 0, 1, "success") == 0);
    CHECK(state.sequence == before && state.counts[6] == 1 && state.counts[7] == 1);
    for (size_t index = 0; index < sizeof(unrecorded) / sizeof(unrecorded[0]); ++index) {
        CHECK(cf_record_locked(&state, unrecorded[index], NULL, "job/file", -1, EPERM, 0, 0, -1, "backing-or-path-error") == 0);
        CHECK(state.sequence == ++before);
    }
    CHECK(cf_record_locked(&state, "fsync", NULL, "job/file", -1, EIO, 0, 0, 1, "backing-or-path-error") == 0);
    CHECK(state.sequence == ++before && state.counts[6] == 1 && state.counts[7] == 1);
    const char *fault_operations[] = {"create", "write", "truncate", "open-trunc"};
    for (size_t index = 0; index < sizeof(fault_operations) / sizeof(fault_operations[0]); ++index) {
        CHECK(cf_record_locked(&state, fault_operations[index], index ? "write" : "create", "job/file", 0, 0, 0, 0, -1, "success") == 0);
        CHECK(state.sequence == ++before);
    }
    pthread_mutex_unlock(&state.mutex);
    clock.now += CF_SUMMARY_NS; cf_watchdog(&state);
    CHECK(state.sequence == before + 1 && state.journal_complete);
    CHECK(control(&state, "STATUS attempt storage instance 0", "\"fsync\":{\"count\":1,\"bytes\":0}") == 0);
    CHECK(control(&state, "STATUS attempt storage instance 0", "\"fdatasync\":{\"count\":1,\"bytes\":0}") == 0);

    /* All eight maximum counters must fit both lifecycle JSON and a maximal-ID STATUS. */
    for (int index = 0; index < CF_COUNTER_COUNT; ++index) state.counts[index] = INT64_MAX;
    state.read_bytes = INT64_MAX;
    pthread_mutex_lock(&state.mutex);
    CHECK(cf_record_locked(&state, "REJECT", NULL, ".", -1, EINVAL, 0, 0, -1, "fixture-rejection") == 0);
    pthread_mutex_unlock(&state.mutex);
    memset(state.attempt, 'a', CF_ID_MAX); state.attempt[CF_ID_MAX] = '\0';
    memset(state.storage, 's', CF_ID_MAX); state.storage[CF_ID_MAX] = '\0';
    memset(state.instance, 'i', CF_ID_MAX); state.instance[CF_ID_MAX] = '\0';
    memset(state.namespace_name, 'n', CF_ID_MAX); state.namespace_name[CF_ID_MAX] = '\0';
    memset(state.fault, 'f', CF_ID_MAX); state.fault[CF_ID_MAX] = '\0';
    char command[CF_COMMAND_MAX], response[CF_RESPONSE_MAX];
    snprintf(command, sizeof(command), "STATUS %s %s %s 0", state.attempt, state.storage, state.instance);
    CHECK(cf_control(&state, command, response, sizeof(response)) == 0);
    CHECK(strstr(response, "\"fdatasync\":{\"count\":9223372036854775807,\"bytes\":0}") != NULL);
    CHECK(strstr(response, "\"read\":{\"count\":9223372036854775807,\"bytes\":9223372036854775807}") != NULL);
    CHECK(response[strlen(response) - 1] == '\n' && strlen(response) < CF_RESPONSE_MAX);
    char journal[CF_RECEIPT_MAX * 2]; ssize_t size = pread(fd, journal, sizeof(journal) - 1, 0);
    CHECK(size > 0); journal[size] = '\0';
    CHECK(strstr(journal, "\"fdatasync\":{\"count\":9223372036854775807,\"bytes\":0}") != NULL);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);
}

static void lost_control_responses(void) {
    struct cf_state state; struct time_source clock = {.now = UINT64_C(1000000000)};
    char path[PATH_MAX], response[CF_RESPONSE_MAX];
    int fd = new_state(&state, &clock, 10000, path);
    const char *readonly[] = {"STATUS attempt storage instance 0", "INVALID", "HEAL attempt storage instance 1 fault",
                             "ARM attempt storage foreign 1 fault 500 create,write"};
    for (size_t i = 0; i < sizeof(readonly) / sizeof(readonly[0]); ++i) {
        bool applied = true;
        (void) cf_control_tracked(&state, readonly[i], response, sizeof(response), &applied);
        CHECK(!applied);
        if (i == 2) CHECK(strstr(response, "not-armed") != NULL);
        cf_control_response_lost(&state, applied);
        CHECK(state.mode == CF_HEALTHY && !state.stop_requested && state.journal_complete);
    }
    CHECK(control(&state, "ARM attempt storage instance 1 fault 500 create,write", "\"ok\":true") == 0);
    bool applied = true;
    CHECK(cf_control_tracked(&state, "STATUS attempt storage instance 1", response, sizeof(response), &applied) == 0);
    cf_control_response_lost(&state, applied); CHECK(!applied && state.mode == CF_ARMED && !state.stop_requested);
    CHECK(cf_control_tracked(&state, "INVALID", response, sizeof(response), &applied) < 0);
    cf_control_response_lost(&state, applied); CHECK(!applied && state.mode == CF_ARMED && !state.stop_requested);
    close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);

    const char *mutations[] = {"ARM attempt storage instance 1 fault 500 create,write", "HEAL attempt storage instance 2 fault",
                              "ABORT attempt storage instance 2 fault", "STOP attempt storage instance 1"};
    for (size_t i = 0; i < sizeof(mutations) / sizeof(mutations[0]); ++i) {
        fd = new_state(&state, &clock, 10000, path);
        if (i == 1 || i == 2) CHECK(control(&state, "ARM attempt storage instance 1 fault 500 create,write", "\"ok\":true") == 0);
        applied = false;
        CHECK(cf_control_tracked(&state, mutations[i], response, sizeof(response), &applied) == 0);
        CHECK(applied);
        cf_control_response_lost(&state, applied);
        CHECK(state.mode == CF_ABORTED && state.stop_requested && state.journal_complete);
        char text[16384]; ssize_t size = pread(fd, text, sizeof(text) - 1, 0); CHECK(size > 0); text[size] = '\0';
        CHECK(strstr(text, "control-response-lost") != NULL);
        close(fd); cf_destroy(&state); CHECK(unlink(path) == 0);
    }
}

static void jail_checks(void) {
    char backing[PATH_MAX], outside[PATH_MAX], secret_path[PATH_MAX];
    snprintf(backing, sizeof(backing), "%s/backing", temporary);
    snprintf(outside, sizeof(outside), "%s/outside", temporary);
    snprintf(secret_path, sizeof(secret_path), "%s/outside/fixture", temporary);
    CHECK(mkdir(backing, 0700) == 0); CHECK(mkdir(outside, 0700) == 0);
    int secret = open(secret_path, O_RDWR | O_CREAT | O_EXCL, 0600); CHECK(secret >= 0); CHECK(write(secret, "private-fixture", 15) == 15);
    struct cj_jail first, second;
    CHECK(cj_init(&first, backing, "job") == 0); CHECK(cj_init(&second, backing, "job") == 0);
    CHECK((fcntl(first.backing_fd, F_GETFL) & O_ACCMODE) == O_RDONLY);
    CHECK(cj_validate(&first, "/") == 0); CHECK(cj_validate(&first, "/job") == 0); CHECK(cj_validate(&first, "/job/chk-1/_metadata") == 0);
    const char *invalid[] = {"/other", "/job-extra/file", "/job/../outside", "/job//file", "/job/./file", "/job/a%2fb", "/job/trailing/", "job/file"};
    for (size_t index = 0; index < sizeof(invalid) / sizeof(invalid[0]); ++index) CHECK(cj_validate(&first, invalid[index]) < 0);
    struct cf_state armed; struct time_source clock = {.now = UINT64_C(1000000000)};
    char journal_path[PATH_MAX]; int journal = new_state(&armed, &clock, 10000, journal_path);
    CHECK(control(&armed, "ARM attempt storage instance 1 fault 500 create,write", "\"ok\":true") == 0);
    pthread_mutex_lock(&armed.mutex);
    CHECK(cj_begin_locked(&first, &armed, "/job/file", "create") == -EIO);
    CHECK(cj_begin_locked(&first, &armed, "/job/file", "write") == -EIO);
    CHECK(cj_begin_locked(&first, &armed, "/job/file", NULL) == 0);
    CHECK(cj_begin_locked(&first, &armed, "/", "create") == -EACCES);
    CHECK(cj_begin_locked(&first, &armed, "/", "write") == -EACCES);
    for (size_t index = 0; index < sizeof(invalid) / sizeof(invalid[0]); ++index) {
        CHECK(cj_begin_locked(&first, &armed, invalid[index], "create") == -EACCES);
        CHECK(cj_begin_locked(&first, &armed, invalid[index], "write") == -EACCES);
    }
    pthread_mutex_unlock(&armed.mutex);
    close(journal); cf_destroy(&armed); CHECK(unlink(journal_path) == 0);
    char leaf[NAME_MAX + 1]; CHECK(cj_parent(&first, "/job", leaf) == -EACCES);
    int fd = cj_open(&first, "/job/data", O_CREAT | O_EXCL | O_RDWR, 0600); CHECK(fd >= 0);
    CHECK(write(fd, "cross-view", 10) == 10); CHECK(fdatasync(fd) == 0); CHECK(fsync(fd) == 0);
    int other = cj_open(&second, "/job/data", O_RDONLY, 0); CHECK(other >= 0);
    char bytes[16] = {0}; CHECK(read(other, bytes, 10) == 10 && !strcmp(bytes, "cross-view")); close(other);
    CHECK(symlinkat(secret_path, first.namespace_fd, "leaf-link") == 0);
    CHECK(cj_open(&first, "/job/leaf-link", O_RDONLY, 0) == -ELOOP);
    struct stat attributes; CHECK(cj_lstat(&first, "/job/leaf-link", &attributes) == -ELOOP);
    CHECK(symlinkat(outside, first.namespace_fd, "directory-link") == 0);
    CHECK(cj_open(&first, "/job/directory-link/fixture", O_RDONLY, 0) < 0);
    CHECK(linkat(AT_FDCWD, secret_path, first.namespace_fd, "hard-link", 0) == 0);
    CHECK(cj_open(&first, "/job/hard-link", O_RDONLY, 0) == -EPERM);
    CHECK(unlinkat(first.namespace_fd, "hard-link", 0) == 0);
    CHECK(unlinkat(first.namespace_fd, "leaf-link", 0) == 0); CHECK(unlinkat(first.namespace_fd, "directory-link", 0) == 0);
    CHECK(mkdirat(first.namespace_fd, "nested", 0700) == 0);
    int parent = cj_parent(&first, "/job/nested/renamed", leaf); CHECK(parent >= 0); CHECK(!strcmp(leaf, "renamed"));
    CHECK(renameat(first.namespace_fd, "data", parent, leaf) == 0); CHECK(fsync(parent) == 0);
    CHECK(cj_lstat(&second, "/job/nested/renamed", &attributes) == 0 && attributes.st_size == 10);
    CHECK(cj_lstat(&second, "/job/data", &attributes) == -ENOENT);
    CHECK(unlinkat(parent, leaf, 0) == 0); close(parent); close(fd);
    CHECK(unlinkat(first.namespace_fd, "nested", AT_REMOVEDIR) == 0);
    CHECK(symlinkat(outside, first.backing_fd, "poisoned") == 0);
    struct cj_jail denied; CHECK(cj_init(&denied, backing, "poisoned") < 0);
    CHECK(unlinkat(first.backing_fd, "poisoned", 0) == 0);
    CHECK(pread(secret, bytes, 15, 0) == 15 && !memcmp(bytes, "private-fixture", 15));
    close(secret); CHECK(unlink(secret_path) == 0);
    cj_destroy(&second); cj_destroy(&first);
    char namespace_path[PATH_MAX]; snprintf(namespace_path, sizeof(namespace_path), "%s/backing/job", temporary);
    CHECK(rmdir(namespace_path) == 0); CHECK(rmdir(backing) == 0); CHECK(rmdir(outside) == 0);
}

int main(void) {
    CHECK(mkdtemp(temporary) != NULL);
    char resolved[PATH_MAX]; CHECK(realpath(temporary, resolved) != NULL);
    CHECK(strlen(resolved) < sizeof(temporary)); strcpy(temporary, resolved);
    CHECK(cf_identifier("attempt-01")); CHECK(!cf_identifier("../escape")); CHECK(!cf_identifier(""));
    uint64_t value; CHECK(cf_uint("0", &value) && value == 0); CHECK(!cf_uint("01", &value));
    CHECK(!cf_uint("18446744073709551616", &value)); CHECK(!cf_uint("-1", &value));
    control_and_held_descriptor(); abort_and_bounds(); aggregate_reads_and_durability(); strict_receipts_and_sync_counters(); lost_control_responses(); jail_checks();
    CHECK(rmdir(temporary) == 0);
    printf("{\"status\":\"pass\",\"tests\":%u,\"scope\":\"native-core-and-jail-no-fuse-mount\"}\n", checks);
    return 0;
}
