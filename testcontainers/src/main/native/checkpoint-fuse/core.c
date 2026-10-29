#define _POSIX_C_SOURCE 200809L
#include "core.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

bool cf_identifier(const char *value) {
    if (!value || !value[0] || strlen(value) > CF_ID_MAX) return false;
    for (const unsigned char *p = (const unsigned char *) value; *p; ++p)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') || *p == '_' || *p == '-')) return false;
    return true;
}

bool cf_uint(const char *value, uint64_t *parsed) {
    if (!value || !value[0] || (value[0] == '0' && value[1])) return false;
    uint64_t number = 0;
    for (const unsigned char *p = (const unsigned char *) value; *p; ++p) {
        if (*p < '0' || *p > '9' || number > (UINT64_MAX - (*p - '0')) / 10) return false;
        number = number * 10 + (*p - '0');
    }
    *parsed = number;
    return true;
}

static uint64_t clock_ns(clockid_t clock_id) {
    struct timespec now;
    if (clock_gettime(clock_id, &now) != 0) abort();
    return (uint64_t) now.tv_sec * UINT64_C(1000000000) + (uint64_t) now.tv_nsec;
}
uint64_t cf_monotonic_ns(void *unused) { (void) unused; return clock_ns(CLOCK_MONOTONIC); }
uint64_t cf_realtime_ns(void *unused) { (void) unused; return clock_ns(CLOCK_REALTIME); }

const char *cf_mode_name(enum cf_mode mode) {
    static const char *const names[] = {"STARTING", "HEALTHY", "ARMED", "EXPIRED", "ABORTED", "FATAL", "STOPPED"};
    return names[mode];
}

static void json_string(char *target, size_t capacity, const char *source) {
    static const char hex[] = "0123456789abcdef";
    size_t written = 0;
    for (const unsigned char *p = (const unsigned char *) source; *p && written + 6 < capacity; ++p) {
        if (*p >= 0x20 && *p < 0x7f && *p != '\\' && *p != '"') target[written++] = (char) *p;
        else {
            target[written++] = '\\'; target[written++] = 'u'; target[written++] = '0'; target[written++] = '0';
            target[written++] = hex[*p >> 4]; target[written++] = hex[*p & 15];
        }
    }
    target[written] = '\0';
}

static const char *const counter_names[CF_COUNTER_COUNT] = {"read", "getattr", "readdir", "access", "flush", "release"};

static int counter_index(const char *operation) {
    for (int i = 0; i < CF_COUNTER_COUNT; ++i) if (!strcmp(operation, counter_names[i])) return i;
    return -1;
}

static bool lifecycle(const char *operation) { return operation[0] >= 'A' && operation[0] <= 'Z'; }

static void counters_json(const struct cf_state *s, char *target, size_t capacity) {
    size_t used = 0;
    for (int i = 0; i < CF_COUNTER_COUNT; ++i) {
        int count = snprintf(target + used, capacity - used, "%s\"%s\":{\"count\":%" PRIu64 ",\"bytes\":%" PRIu64 "}",
                i ? "," : "", counter_names[i], s->counts[i], i == 0 ? s->read_bytes : 0);
        if (count < 0 || (size_t) count >= capacity - used) abort();
        used += (size_t) count;
    }
}

static bool checkpoint_read(const char *path) {
    if (!path) return false;
    for (const char *p = path; *p; ++p) {
        if ((p == path || p[-1] == '/') && !strncmp(p, "chk-", 4) && p[4] >= '1' && p[4] <= '9') {
            const char *end = p + 5;
            while (*end >= '0' && *end <= '9') ++end;
            if (*end == '/') return true;
        }
    }
    return false;
}

static int append_receipt(struct cf_state *s, const char *operation, const char *fault_operation,
                          const char *path, int64_t result, int error, uint64_t requested,
                          uint64_t returned, int datasync, const char *reason) {
    char encoded[CF_PATH_MAX * 6 + 1], fault[CF_ID_MAX + 3], fault_op[24], line[CF_RECEIPT_MAX];
    char bounded_path[CF_PATH_MAX + 1], counters[512], extra[560] = "";
    bool truncated = path && strlen(path) > CF_PATH_MAX;
    snprintf(bounded_path, sizeof(bounded_path), "%.*s", CF_PATH_MAX, path ? path : ".");
    json_string(encoded, sizeof(encoded), bounded_path);
    if (lifecycle(operation)) {
        counters_json(s, counters, sizeof(counters));
        snprintf(extra, sizeof(extra), ",\"counters\":{%s}", counters);
    }
    if (s->fault[0]) snprintf(fault, sizeof(fault), "\"%s\"", s->fault); else strcpy(fault, "null");
    if (fault_operation) snprintf(fault_op, sizeof(fault_op), "\"%s\"", fault_operation); else strcpy(fault_op, "null");
    uint64_t next = s->sequence + 1;
    int count = snprintf(line, sizeof(line),
        "{\"version\":1,\"sequence\":%" PRIu64 ",\"timestampMs\":%" PRIu64 ",\"monotonicNs\":%" PRIu64
        ",\"daemonPid\":%ld,\"attemptId\":\"%s\",\"storageId\":\"%s\",\"instanceId\":\"%s\",\"namespace\":\"%s\""
        ",\"faultId\":%s,\"revision\":%" PRIu64 ",\"state\":\"%s\",\"journalComplete\":%s"
        ",\"leaseMs\":%" PRIu64 ",\"leaseDeadlineMonotonicNs\":%" PRIu64 ",\"allowedOperations\":%s"
        ",\"operation\":\"%s\",\"path\":\"%s\",\"faultOperation\":%s,\"result\":%" PRId64
        ",\"errno\":%d,\"requestedBytes\":%" PRIu64 ",\"returnedBytes\":%" PRIu64 ",\"datasync\":%s,\"reason\":\"%s\"%s%s}\n",
        next, s->realtime(s->clock_context) / UINT64_C(1000000), s->monotonic(s->clock_context),
        (long) s->daemon_pid, s->attempt, s->storage, s->instance, s->namespace_name,
        fault, s->revision, cf_mode_name(s->mode), s->journal_complete ? "true" : "false",
        s->lease_ms, s->deadline_ns, s->fault[0] ? "[\"create\",\"write\"]" : "[]",
        operation, encoded, fault_op, result, error, requested, returned,
        datasync < 0 ? "null" : datasync ? "true" : "false", reason ? reason : "none",
        truncated ? ",\"pathTruncated\":true" : "", extra);
    if (count < 0 || (size_t) count >= sizeof(line) || s->sequence >= s->max_records ||
        (uint64_t) count > s->max_bytes - s->bytes_written) return -ENOSPC;
    size_t done = 0;
    while (done < (size_t) count) {
        ssize_t amount = write(s->journal_fd, line + done, (size_t) count - done);
        if (amount < 0 && errno == EINTR) continue;
        if (amount <= 0) return -EIO;
        done += (size_t) amount;
        s->bytes_written += (uint64_t) amount;
    }
    /* The record is already appended: a failed sync must not reuse this sequence. */
    s->sequence = next;
    if (lifecycle(operation)) { s->pending_aggregate = 0; s->last_summary_ns = s->monotonic(s->clock_context); }
    /* Fault-family receipts and control/lifecycle acknowledgements are durable. Successful
       reads/metadata and periodic summaries never add a synchronous disk flush per callback. */
    if ((fault_operation || (lifecycle(operation) && strcmp(operation, "SUMMARY"))) && fdatasync(s->journal_fd) != 0) return -EIO;
    return 0;
}

static void fatal_locked(struct cf_state *s, const char *reason) {
    if (s->mode == CF_FATAL) return;
    s->mode = CF_FATAL;
    s->journal_complete = false;
    /* One reserved receipt slot makes budget exhaustion visible without overwriting history. */
    (void) append_receipt(s, "FATAL", NULL, ".", -1, EIO, 0, 0, -1, reason);
}

static int reserve_locked(struct cf_state *s) {
    if (s->mode == CF_FATAL) return -EIO;
    if (s->sequence + 1 >= s->max_records || s->bytes_written + 2 * CF_RECEIPT_MAX > s->max_bytes) {
        fatal_locked(s, "journal-bound-exceeded");
        return -EIO;
    }
    return 0;
}

static int summary_locked(struct cf_state *s) {
    if (!s->pending_aggregate || s->mode == CF_FATAL) return 0;
    uint64_t now = s->monotonic(s->clock_context);
    if (s->pending_aggregate < CF_SUMMARY_OPERATIONS && now - s->last_summary_ns < CF_SUMMARY_NS) return 0;
    if (reserve_locked(s) != 0) return -EIO;
    if (append_receipt(s, "SUMMARY", NULL, ".", 0, 0, 0, 0, -1, "successful-io-counters") != 0) {
        fatal_locked(s, "journal-write-failed"); return -EIO;
    }
    return 0;
}

int cf_record_locked(struct cf_state *s, const char *operation, const char *fault_operation,
                     const char *path, int64_t result, int error, uint64_t requested,
                     uint64_t returned, int datasync, const char *reason) {
    int counter = counter_index(operation);
    if (counter >= 0 && result >= 0 && !error) {
        if (s->counts[counter] == INT64_MAX || (counter == 0 && returned > (uint64_t) INT64_MAX - s->read_bytes)) {
            fatal_locked(s, "counter-bound-exceeded"); return -EIO;
        }
        ++s->counts[counter]; ++s->pending_aggregate;
        if (counter == 0) s->read_bytes += returned;
        if (counter != 0 || !checkpoint_read(path)) return summary_locked(s);
    }
    if (reserve_locked(s) != 0) return -EIO;
    if (path && strlen(path) > CF_PATH_MAX) { result = -1; error = ENAMETOOLONG; reason = "receipt-path-truncated"; }
    if (append_receipt(s, operation, fault_operation, path, result, error, requested, returned, datasync, reason) != 0) {
        fatal_locked(s, "journal-write-failed");
        return -EIO;
    }
    return 0;
}

static void expire_locked(struct cf_state *s) {
    if (s->mode != CF_ARMED || s->monotonic(s->clock_context) < s->deadline_ns) return;
    if (reserve_locked(s) != 0) return;
    s->mode = CF_EXPIRED;
    (void) cf_record_locked(s, "EXPIRE", NULL, ".", -1, ETIMEDOUT, 0, 0, -1, "lease-expired");
}

int cf_begin_operation_locked(struct cf_state *s, const char *fault_operation, bool mutation) {
    expire_locked(s);
    if (mutation && reserve_locked(s) != 0) return -EIO;
    if (s->stop_requested || s->mode == CF_STOPPED) return -EIO;
    if (s->mode == CF_STARTING) return -EAGAIN;
    if (fault_operation && (s->mode == CF_ARMED || s->mode == CF_EXPIRED || s->mode == CF_ABORTED)) return -EIO;
    return 0;
}

int cf_begin_locked(struct cf_state *s, const char *fault_operation) {
    return cf_begin_operation_locked(s, fault_operation, fault_operation != NULL);
}

int cf_init(struct cf_state *s, const char *attempt, const char *storage, const char *instance,
            const char *namespace_name, int journal_fd, uint64_t max_bytes, uint64_t max_records,
            cf_clock monotonic, cf_clock realtime, void *clock_context) {
    if (!cf_identifier(attempt) || !cf_identifier(storage) || !cf_identifier(instance) ||
        !cf_identifier(namespace_name) || journal_fd < 0 || max_bytes < 4 * CF_RECEIPT_MAX ||
        max_bytes > 64 * 1024 * 1024 || max_records < 10 || max_records > 100000) return -EINVAL;
    memset(s, 0, sizeof(*s));
    int error = pthread_mutex_init(&s->mutex, NULL);
    if (error) return -error;
    strcpy(s->attempt, attempt); strcpy(s->storage, storage); strcpy(s->instance, instance);
    strcpy(s->namespace_name, namespace_name);
    s->mode = CF_STARTING; s->journal_fd = journal_fd; s->journal_complete = true;
    s->max_bytes = max_bytes; s->max_records = max_records; s->daemon_pid = getpid();
    s->monotonic = monotonic ? monotonic : cf_monotonic_ns;
    s->realtime = realtime ? realtime : cf_realtime_ns; s->clock_context = clock_context;
    pthread_mutex_lock(&s->mutex);
    int result = cf_record_locked(s, "START", NULL, ".", 0, 0, 0, 0, -1, "instance-created");
    pthread_mutex_unlock(&s->mutex);
    return result;
}

void cf_destroy(struct cf_state *s) { pthread_mutex_destroy(&s->mutex); }

int cf_ready(struct cf_state *s) {
    pthread_mutex_lock(&s->mutex);
    int result = -EINVAL;
    if (s->mode == CF_STARTING && reserve_locked(s) == 0) {
        s->mode = CF_HEALTHY;
        result = cf_record_locked(s, "READY", NULL, ".", 0, 0, 0, 0, -1,
                "direct_io=1,writeback_cache=0,entry_timeout=0,attr_timeout=0,negative_timeout=0,kernel_cache=0,auto_cache=0");
    }
    pthread_mutex_unlock(&s->mutex);
    return result;
}

void cf_watchdog(struct cf_state *s) {
    pthread_mutex_lock(&s->mutex); expire_locked(s); (void) summary_locked(s); pthread_mutex_unlock(&s->mutex);
}

void cf_shutdown(struct cf_state *s, const char *reason) {
    pthread_mutex_lock(&s->mutex);
    if (!s->stop_requested && s->mode != CF_FATAL && reserve_locked(s) == 0) {
        if (s->mode != CF_EXPIRED) s->mode = CF_ABORTED;
        (void) cf_record_locked(s, "ABORT", NULL, ".", -1, ECANCELED, 0, 0, -1, reason);
    }
    s->stop_requested = true;
    pthread_mutex_unlock(&s->mutex);
}

static int response_locked(struct cf_state *s, bool ok, const char *error, char *response, size_t capacity) {
    char fault[CF_ID_MAX + 3], encoded_error[96], counters[512];
    counters_json(s, counters, sizeof(counters));
    if (s->fault[0]) snprintf(fault, sizeof(fault), "\"%s\"", s->fault); else strcpy(fault, "null");
    if (error) snprintf(encoded_error, sizeof(encoded_error), "\"%s\"", error); else strcpy(encoded_error, "null");
    uint64_t now = s->monotonic(s->clock_context);
    uint64_t remaining = s->mode == CF_ARMED && s->deadline_ns > now
        ? (s->deadline_ns - now + UINT64_C(999999)) / UINT64_C(1000000) : 0;
    int count = snprintf(response, capacity,
        "{\"version\":1,\"ok\":%s,\"error\":%s,\"state\":\"%s\",\"attemptId\":\"%s\",\"storageId\":\"%s\","
        "\"instanceId\":\"%s\",\"namespace\":\"%s\",\"faultId\":%s,\"revision\":%" PRIu64
        ",\"sequence\":%" PRIu64 ",\"journalComplete\":%s,\"daemonPid\":%ld,\"leaseRemainingMs\":%" PRIu64
        ",\"leaseMs\":%" PRIu64 ",\"leaseDeadlineMonotonicNs\":%" PRIu64 ",\"allowedOperations\":%s,\"stopping\":%s,\"counters\":{%s}}\n",
        ok ? "true" : "false", encoded_error, cf_mode_name(s->mode), s->attempt, s->storage, s->instance,
        s->namespace_name, fault, s->revision, s->sequence, s->journal_complete ? "true" : "false",
        (long) s->daemon_pid, remaining, s->lease_ms, s->deadline_ns,
        s->fault[0] ? "[\"create\",\"write\"]" : "[]", s->stop_requested ? "true" : "false", counters);
    return count > 0 && (size_t) count < capacity ? (ok ? 0 : -EINVAL) : -ENOSPC;
}

int cf_control(struct cf_state *s, const char *command, char *response, size_t capacity) {
    char copy[CF_COMMAND_MAX + 1], *tokens[9] = {0};
    size_t number = 0;
    const char *error = NULL;
    uint64_t revision = 0, lease = 0;
    if (!command || strlen(command) > CF_COMMAND_MAX) error = "malformed-command";
    else {
        strcpy(copy, command);
        for (const unsigned char *p = (const unsigned char *) copy; *p; ++p)
            if (*p < 0x20 || *p >= 0x7f) error = "malformed-command";
        char *save = NULL;
        for (char *token = strtok_r(copy, " ", &save); token; token = strtok_r(NULL, " ", &save)) {
            if (number == 9) { error = "malformed-command"; break; }
            tokens[number++] = token;
        }
        if (number < 5 || !cf_uint(tokens[4], &revision)) error = "malformed-command";
    }
    pthread_mutex_lock(&s->mutex);
    expire_locked(s);
    bool status = !error && strcmp(tokens[0], "STATUS") == 0;
    bool stop = !error && strcmp(tokens[0], "STOP") == 0;
    if (!error && (!cf_identifier(tokens[1]) || !cf_identifier(tokens[2]) || !cf_identifier(tokens[3]))) error = "malformed-command";
    if (!error && (strcmp(tokens[1], s->attempt) || strcmp(tokens[2], s->storage) || strcmp(tokens[3], s->instance))) error = "foreign-identity";
    if (!error && ((status && revision != s->revision) || (!status && (s->revision == UINT64_MAX || revision != s->revision + 1)))) error = "stale-revision";
    if (!error && ((status || stop) ? number != 5 : !strcmp(tokens[0], "ARM") ? number != 8 :
        (!strcmp(tokens[0], "HEAL") || !strcmp(tokens[0], "ABORT")) ? number != 6 : true)) error = "malformed-command";
    if (!error && !status && !stop && !cf_identifier(tokens[5])) error = "malformed-command";
    if (!error && !status && !stop && s->mode != CF_HEALTHY && s->mode != CF_ARMED) error = "terminal-state";
    if (!error && !strcmp(tokens[0], "ARM") &&
        (s->mode != CF_HEALTHY || !cf_uint(tokens[6], &lease) || lease < 1 || lease > CF_MAX_LEASE_MS ||
         strcmp(tokens[7], "create,write") || UINT64_MAX - s->monotonic(s->clock_context) < lease * UINT64_C(1000000))) error = "invalid-arm";
    if (!error && (!strcmp(tokens[0], "HEAL") || !strcmp(tokens[0], "ABORT")) &&
        (s->mode != CF_ARMED || strcmp(tokens[5], s->fault))) error = "fault-id-mismatch";
    if (!error && !status && !stop && s->stop_requested) error = "terminal-state";
    /* Terminal status/stop remain available even when no further receipt can be stored. */
    if (s->mode == CF_FATAL && !status && !stop && !error) error = "journal-incomplete";
    if (!status && s->mode != CF_FATAL && reserve_locked(s) != 0 && !stop) error = "journal-incomplete";
    if (error) {
        if (s->mode != CF_FATAL) (void) cf_record_locked(s, "REJECT", NULL, ".", -1, EINVAL, 0, 0, -1, error);
    } else if (!status) {
        s->revision = revision;
        if (stop) {
            if (s->mode == CF_ARMED) {
                s->mode = CF_ABORTED;
                (void) cf_record_locked(s, "ABORT", NULL, ".", -1, ECANCELED, 0, 0, -1, "stop-while-armed");
            } else if (s->mode == CF_HEALTHY || s->mode == CF_STARTING) s->mode = CF_STOPPED;
            s->stop_requested = true;
        } else if (!strcmp(tokens[0], "ARM")) {
            strcpy(s->fault, tokens[5]); s->mode = CF_ARMED;
            s->lease_ms = lease;
            s->deadline_ns = s->monotonic(s->clock_context) + lease * UINT64_C(1000000);
        } else if (!strcmp(tokens[0], "HEAL")) s->mode = CF_HEALTHY;
        else s->mode = CF_ABORTED;
        if (s->mode != CF_FATAL && cf_record_locked(s, tokens[0], NULL, ".", 0, 0, 0, 0, -1, "accepted") != 0) error = "journal-incomplete";
    }
    if (s->mode == CF_FATAL && !status && !stop) error = "journal-incomplete";
    int result = response_locked(s, !error, error, response, capacity);
    pthread_mutex_unlock(&s->mutex);
    return result;
}
