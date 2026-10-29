#ifndef CHECKPOINT_FUSE_CORE_H
#define CHECKPOINT_FUSE_CORE_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define CF_ID_MAX 64
#define CF_PATH_MAX 1024
#define CF_COMMAND_MAX 512
#define CF_RESPONSE_MAX 2048
#define CF_RECEIPT_MAX 16384
#define CF_MAX_LEASE_MS 120000
#define CF_COUNTER_COUNT 6
#define CF_SUMMARY_OPERATIONS 1000
#define CF_SUMMARY_NS UINT64_C(1000000000)

enum cf_mode { CF_STARTING, CF_HEALTHY, CF_ARMED, CF_EXPIRED, CF_ABORTED, CF_FATAL, CF_STOPPED };
typedef uint64_t (*cf_clock)(void *context);

struct cf_state {
    pthread_mutex_t mutex;
    char attempt[CF_ID_MAX + 1], storage[CF_ID_MAX + 1], instance[CF_ID_MAX + 1];
    char namespace_name[CF_ID_MAX + 1], fault[CF_ID_MAX + 1];
    enum cf_mode mode;
    uint64_t revision, sequence, deadline_ns, lease_ms, bytes_written, max_bytes, max_records;
    uint64_t counts[CF_COUNTER_COUNT], read_bytes, pending_aggregate, last_summary_ns;
    bool journal_complete, stop_requested;
    int journal_fd;
    pid_t daemon_pid;
    cf_clock monotonic, realtime;
    void *clock_context;
};

bool cf_identifier(const char *value);
bool cf_uint(const char *value, uint64_t *parsed);
uint64_t cf_monotonic_ns(void *unused);
uint64_t cf_realtime_ns(void *unused);
const char *cf_mode_name(enum cf_mode mode);
int cf_init(struct cf_state *state, const char *attempt, const char *storage, const char *instance,
            const char *namespace_name, int journal_fd, uint64_t max_bytes, uint64_t max_records,
            cf_clock monotonic, cf_clock realtime, void *clock_context);
void cf_destroy(struct cf_state *state);
int cf_ready(struct cf_state *state);
void cf_watchdog(struct cf_state *state);
void cf_shutdown(struct cf_state *state, const char *reason);

/* FUSE calls hold this mutex from authorization through the backing syscall and receipt. */
int cf_begin_locked(struct cf_state *state, const char *fault_operation);
int cf_begin_operation_locked(struct cf_state *state, const char *fault_operation, bool mutation);
int cf_record_locked(struct cf_state *state, const char *operation, const char *fault_operation,
                     const char *path, int64_t result, int error, uint64_t requested,
                     uint64_t returned, int datasync, const char *reason);
int cf_control(struct cf_state *state, const char *command, char *response, size_t capacity);

#endif
