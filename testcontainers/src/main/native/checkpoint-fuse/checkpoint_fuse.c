#define _GNU_SOURCE
#define FUSE_USE_VERSION 35
#include <fuse3/fuse.h>
#include <fuse3/fuse_lowlevel.h>
#include "core.h"
#include "jail.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

struct handle { int fd; DIR *directory; bool virtual_root; char path[CF_PATH_MAX + 1]; };
static struct cf_state state;
static struct cj_jail jail;
static struct fuse *filesystem;
static atomic_bool mounted;
static unsigned int view_uid, view_gid, handles;
static int listener = -1;

static const char *relative(const char *path) { return !path || !strcmp(path, "/") ? "." : path + 1; }
static struct handle *handle_of(struct fuse_file_info *info) { return info ? (struct handle *) (uintptr_t) info->fh : NULL; }
static const char *effective_path(const char *path, struct fuse_file_info *info) {
    struct handle *h = handle_of(info);
    return path ? path : h ? h->path : "/";
}

static int finish(const char *path, const char *operation, const char *family, int result,
                  uint64_t requested, uint64_t returned, int datasync) {
    const char *reason = result >= 0 ? "success" : result == -EIO && family &&
        (state.mode == CF_ARMED || state.mode == CF_EXPIRED || state.mode == CF_ABORTED)
        ? "eio-rule" : "backing-or-path-error";
    if (cf_record_locked(&state, operation, family, relative(path), result < 0 ? -1 : result,
            result < 0 ? -result : 0, requested, returned, datasync, reason) != 0) result = -EIO;
    pthread_mutex_unlock(&state.mutex);
    return result;
}

static int begin(const char *path, const char *family) {
    pthread_mutex_lock(&state.mutex);
    return cj_begin_locked(&jail, &state, path, family);
}

static void view_attributes(const char *path, struct stat *attributes) {
    attributes->st_uid = view_uid; attributes->st_gid = view_gid;
    if (!strcmp(path, "/")) attributes->st_mode = S_IFDIR | 0755;
}

static int get_attributes(const char *path, struct stat *attributes, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result) {
        if (h && !h->virtual_root) result = fstat(h->fd, attributes) == 0 ? cj_type(attributes) : -errno;
        else result = cj_lstat(&jail, path, attributes);
    }
    if (!result) view_attributes(path, attributes);
    return finish(path, "getattr", NULL, result, 0, 0, -1);
}

static int new_handle(const char *path, int fd, struct fuse_file_info *info, bool directory, bool root) {
    if (handles >= 256) { if (fd >= 0) close(fd); return -EMFILE; }
    struct handle *h = calloc(1, sizeof(*h));
    if (!h) { if (fd >= 0) close(fd); return -ENOMEM; }
    h->fd = fd; h->virtual_root = root; strcpy(h->path, path);
    if (directory && !root) {
        h->directory = fdopendir(fd);
        if (!h->directory) { int error = errno; close(fd); free(h); return -error; }
    }
    info->fh = (uint64_t) (uintptr_t) h;
    info->direct_io = 1; info->keep_cache = 0;
    ++handles;
    return 0;
}

static int open_file(const char *path, struct fuse_file_info *info) {
    const char *family = info->flags & O_TRUNC ? "write" : NULL;
    int result = begin(path, family);
    if (!result) {
        int fd = cj_open(&jail, path, info->flags & ~(O_CREAT | O_EXCL), 0);
        result = fd < 0 ? fd : new_handle(path, fd, info, false, false);
    }
    return finish(path, "open", family, result, 0, 0, -1);
}

static int create_file(const char *path, mode_t mode, struct fuse_file_info *info) {
    int result = begin(path, "create");
    if (!result) {
        int fd = cj_open(&jail, path, info->flags | O_CREAT, mode);
        result = fd < 0 ? fd : new_handle(path, fd, info, false, false);
    }
    return finish(path, "create", "create", result, 0, 0, -1);
}

static int read_file(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result && (!h || h->directory || h->virtual_root || size > INT_MAX || offset < 0)) result = -EINVAL;
    if (!result) { ssize_t count = pread(h->fd, buffer, size, offset); result = count < 0 ? -errno : (int) count; }
    return finish(path, "read", NULL, result, size, result > 0 ? (uint64_t) result : 0, -1);
}

static int write_file(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, "write");
    struct handle *h = handle_of(info);
    if (!result && (!h || h->directory || h->virtual_root || size > INT_MAX || offset < 0)) result = -EINVAL;
    if (!result) { ssize_t count = pwrite(h->fd, buffer, size, offset); result = count < 0 ? -errno : (int) count; }
    return finish(path, "write", "write", result, size, result > 0 ? (uint64_t) result : 0, -1);
}

static int sync_file(const char *path, int datasync, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result && !h) result = -EBADF;
    if (!result) {
        int fd = h->virtual_root ? jail.backing_fd : h->fd;
        if ((datasync ? fdatasync(fd) : fsync(fd)) != 0) result = -errno;
    }
    return finish(path, h && (h->directory || h->virtual_root) ? "fsyncdir" : "fsync", NULL, result, 0, 0, !!datasync);
}

static int flush_file(const char *path, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result && !h) result = -EBADF;
    if (!result) {
        int duplicate = dup(h->fd);
        if (duplicate < 0) result = -errno;
        else if (close(duplicate) != 0) result = -errno;
    }
    return finish(path, "flush", NULL, result, 0, 0, -1);
}

static int release_file(const char *path, struct fuse_file_info *info) {
    path = effective_path(path, info);
    char retained[CF_PATH_MAX + 1]; snprintf(retained, sizeof(retained), "%s", path);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (h) {
        int closed = h->directory ? closedir(h->directory) : h->fd >= 0 ? close(h->fd) : 0;
        if (closed != 0 && !result) result = -errno;
        free(h); info->fh = 0; --handles;
    } else if (!result) result = -EBADF;
    return finish(retained, "release", NULL, result, 0, 0, -1);
}

static int open_directory(const char *path, struct fuse_file_info *info) {
    int result = begin(path, NULL);
    if (!result) {
        bool root = !strcmp(path, "/");
        int fd = root ? -1 : cj_open(&jail, path, O_RDONLY | O_DIRECTORY, 0);
        result = !root && fd < 0 ? fd : new_handle(path, fd, info, true, root);
    }
    return finish(path, "opendir", NULL, result, 0, 0, -1);
}

static int read_directory(const char *path, void *buffer, fuse_fill_dir_t fill, off_t offset,
                          struct fuse_file_info *info, enum fuse_readdir_flags flags) {
    (void) flags;
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result && (!h || offset < 0)) result = -EINVAL;
    if (!result && h->virtual_root) {
        const char *names[] = {".", "..", jail.namespace_name};
        for (off_t index = offset; index < 3; ++index)
            if (fill(buffer, names[index], NULL, index + 1, 0)) break;
    } else if (!result && !h->directory) result = -ENOTDIR;
    else if (!result) {
        if (!offset) rewinddir(h->directory); else seekdir(h->directory, offset);
        errno = 0;
        struct dirent *entry;
        while ((entry = readdir(h->directory)) != NULL) {
            struct stat attributes;
            if (fstatat(h->fd, entry->d_name, &attributes, AT_SYMLINK_NOFOLLOW) != 0) { result = -errno; break; }
            if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..")) {
                result = cj_type(&attributes);
                if (result) break;
            }
            view_attributes(path, &attributes);
            off_t next = telldir(h->directory);
            if (next < 0) { result = -errno; break; }
            if (fill(buffer, entry->d_name, &attributes, next, 0)) break;
            errno = 0;
        }
        if (!entry && errno && !result) result = -errno;
    }
    return finish(path, "readdir", NULL, result, 0, 0, -1);
}

static int make_directory(const char *path, mode_t mode) {
    int result = begin(path, NULL);
    if (!result) {
        char leaf[NAME_MAX + 1]; int parent = cj_parent(&jail, path, leaf);
        if (parent < 0) result = parent;
        else { if (mkdirat(parent, leaf, mode & 0777) != 0) result = -errno; close(parent); }
    }
    return finish(path, "mkdir", NULL, result, 0, 0, -1);
}

static int remove_path(const char *path, bool directory) {
    int result = begin(path, NULL);
    struct stat attributes;
    if (!result) result = cj_lstat(&jail, path, &attributes);
    if (!result) {
        char leaf[NAME_MAX + 1]; int parent = cj_parent(&jail, path, leaf);
        if (parent < 0) result = parent;
        else { if (unlinkat(parent, leaf, directory ? AT_REMOVEDIR : 0) != 0) result = -errno; close(parent); }
    }
    return finish(path, directory ? "rmdir" : "unlink", NULL, result, 0, 0, -1);
}
static int remove_file(const char *path) { return remove_path(path, false); }
static int remove_directory(const char *path) { return remove_path(path, true); }

static int rename_path(const char *source, const char *target, unsigned int flags) {
    int result = begin(source, NULL);
    if (!result) result = cj_validate(&jail, target);
    if (!result && flags != 0 && flags != RENAME_NOREPLACE) result = -EOPNOTSUPP;
    struct stat attributes;
    if (!result) result = cj_lstat(&jail, source, &attributes);
    if (!result) { int checked = cj_lstat(&jail, target, &attributes); if (checked != -ENOENT) result = checked; }
    if (!result) {
        char old_name[NAME_MAX + 1], new_name[NAME_MAX + 1];
        int old_parent = cj_parent(&jail, source, old_name), new_parent = cj_parent(&jail, target, new_name);
        if (old_parent < 0) result = old_parent;
        else if (new_parent < 0) result = new_parent;
        else if (renameat2(old_parent, old_name, new_parent, new_name, flags) != 0) result = -errno;
        if (old_parent >= 0) close(old_parent);
        if (new_parent >= 0) close(new_parent);
    }
    /* Each target has its own bounded receipt, making both rename paths auditable. */
    if (cf_record_locked(&state, "rename-target", NULL, relative(target), result < 0 ? -1 : 0,
                         result < 0 ? -result : 0, 0, 0, -1, "rename-destination") != 0) result = -EIO;
    return finish(source, "rename", NULL, result, 0, 0, -1);
}

static int truncate_file(const char *path, off_t size, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, "write");
    struct handle *h = handle_of(info);
    if (!result && size < 0) result = -EINVAL;
    if (!result) {
        int fd = h ? h->fd : cj_open(&jail, path, O_WRONLY, 0);
        if (fd < 0) result = fd;
        else { if (ftruncate(fd, size) != 0) result = -errno; if (!h) close(fd); }
    }
    return finish(path, "truncate", "write", result, 0, 0, -1);
}

static int chmod_file(const char *path, mode_t mode, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result && (!strcmp(path, "/") || cj_namespace_root(&jail, path))) result = -EPERM;
    if (!result) {
        int fd = h ? h->fd : cj_open(&jail, path, O_RDONLY | O_NONBLOCK, 0);
        if (fd < 0) result = fd;
        else { if (fchmod(fd, mode & 0777) != 0) result = -errno; if (!h) close(fd); }
    }
    return finish(path, "chmod", NULL, result, 0, 0, -1);
}

static int chown_file(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct stat attributes;
    if (!result) result = cj_lstat(&jail, path, &attributes);
    if (!result && ((uid != (uid_t) -1 && uid != view_uid) || (gid != (gid_t) -1 && gid != view_gid))) result = -EPERM;
    /* View ownership is fixed; backing objects remain owned by the daemon. */
    return finish(path, "chown", NULL, result, 0, 0, -1);
}

static int change_times(const char *path, const struct timespec times[2], struct fuse_file_info *info) {
    path = effective_path(path, info);
    int result = begin(path, NULL);
    struct handle *h = handle_of(info);
    if (!result) {
        bool namespace_root = cj_namespace_root(&jail, path);
        int fd = h ? h->fd : namespace_root ? dup(jail.namespace_fd) : cj_open(&jail, path, O_RDONLY | O_NONBLOCK, 0);
        if (fd < 0) result = h ? -EBADF : namespace_root ? -errno : fd;
        else { if (futimens(fd, times) != 0) result = -errno; if (!h) close(fd); }
    }
    return finish(path, "utimens", NULL, result, 0, 0, -1);
}

static int filesystem_attributes(const char *path, struct statvfs *attributes) {
    int result = begin(path, NULL);
    if (!result && fstatvfs(jail.namespace_fd, attributes) != 0) result = -errno;
    return finish(path, "statfs", NULL, result, 0, 0, -1);
}

static int access_file(const char *path, int mask) {
    (void) mask;
    int result = begin(path, NULL);
    struct stat attributes;
    if (!result) result = cj_lstat(&jail, path, &attributes);
    return finish(path, "access", NULL, result, 0, 0, -1);
}

static int deny(const char *path, const char *operation) {
    int result = begin(path, NULL);
    return finish(path, operation, NULL, result ? result : -EPERM, 0, 0, -1);
}
static int deny_symlink(const char *target, const char *path) { (void) target; return deny(path, "symlink"); }
static int deny_link(const char *source, const char *path) { (void) source; return deny(path, "link"); }
static int deny_readlink(const char *path, char *buffer, size_t size) { (void) buffer; (void) size; return deny(path, "readlink"); }
static int deny_mknod(const char *path, mode_t mode, dev_t device) { (void) mode; (void) device; return deny(path, "mknod"); }

static void *initialize(struct fuse_conn_info *connection, struct fuse_config *config) {
    connection->want &= ~FUSE_CAP_WRITEBACK_CACHE;
    if (connection->max_write > 65536) connection->max_write = 65536;
    config->direct_io = 1; config->kernel_cache = 0; config->auto_cache = 0;
    config->entry_timeout = 0; config->attr_timeout = 0; config->negative_timeout = 0;
    config->set_uid = 1; config->uid = view_uid; config->set_gid = 1; config->gid = view_gid;
    config->nullpath_ok = 0; config->use_ino = 0;
    if (cf_ready(&state) != 0) fuse_exit(filesystem);
    return &state;
}

static const struct fuse_operations operations = {
    .init = initialize, .getattr = get_attributes, .open = open_file, .create = create_file,
    .read = read_file, .write = write_file, .flush = flush_file, .release = release_file,
    .fsync = sync_file, .opendir = open_directory, .readdir = read_directory, .releasedir = release_file,
    .fsyncdir = sync_file, .mkdir = make_directory, .unlink = remove_file, .rmdir = remove_directory,
    .rename = rename_path, .truncate = truncate_file, .chmod = chmod_file, .chown = chown_file,
    .utimens = change_times, .statfs = filesystem_attributes, .access = access_file,
    .symlink = deny_symlink, .link = deny_link, .readlink = deny_readlink, .mknod = deny_mknod
};

static void unmount_once(void) {
    if (atomic_exchange(&mounted, false)) fuse_unmount(filesystem);
}

static int send_all(int fd, const char *text) {
    size_t length = strlen(text), sent = 0;
    while (sent < length) {
        ssize_t amount = send(fd, text + sent, length - sent, MSG_NOSIGNAL);
        if (amount < 0 && errno == EINTR) continue;
        if (amount <= 0) return -1;
        sent += (size_t) amount;
    }
    return 0;
}

static void *control_loop(void *unused) {
    (void) unused;
    for (;;) {
        cf_watchdog(&state);
        pthread_mutex_lock(&state.mutex); bool stopping = state.stop_requested; pthread_mutex_unlock(&state.mutex);
        if (stopping) break;
        struct pollfd poller = {.fd = listener, .events = POLLIN};
        int selected = poll(&poller, 1, 50);
        if (selected < 0 && errno == EINTR) continue;
        if (selected < 0) { cf_shutdown(&state, "control-poll-failed"); break; }
        if (!selected || !(poller.revents & POLLIN)) continue;
        int client = accept4(listener, NULL, NULL, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (client < 0) continue;
        struct timeval timeout = {.tv_sec = 0, .tv_usec = 250000};
        (void) setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        (void) setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        struct ucred credentials; socklen_t credentials_size = sizeof(credentials);
        char command[CF_COMMAND_MAX + 2], response[CF_RESPONSE_MAX];
        size_t used = 0; bool complete = false;
        if (getsockopt(client, SOL_SOCKET, SO_PEERCRED, &credentials, &credentials_size) == 0 && credentials.uid == 0) {
            uint64_t read_deadline = cf_monotonic_ns(NULL) + UINT64_C(250000000);
            while (used < sizeof(command) - 1) {
                uint64_t now = cf_monotonic_ns(NULL);
                if (now >= read_deadline) break;
                struct pollfd input = {.fd = client, .events = POLLIN};
                int available = poll(&input, 1, (int) ((read_deadline - now + 999999) / 1000000));
                if (available < 0 && errno == EINTR) continue;
                if (available <= 0) break;
                ssize_t amount = recv(client, command + used, sizeof(command) - 1 - used, 0);
                if (amount < 0 && (errno == EINTR || errno == EAGAIN)) continue;
                if (amount <= 0) break;
                used += (size_t) amount;
                char *newline = memchr(command, '\n', used);
                if (newline) {
                    complete = (size_t) (newline - command) + 1 == used;
                    for (char *p = command; p < newline; ++p) if ((unsigned char) *p < 0x20 || (unsigned char) *p >= 0x7f) complete = false;
                    *newline = '\0'; break;
                }
            }
        }
        if (!complete) strcpy(command, "INVALID");
        (void) cf_control(&state, command, response, sizeof(response));
        if (send_all(client, response) != 0) cf_shutdown(&state, "control-response-lost");
        close(client);
    }
    fuse_exit(filesystem);
    unmount_once();
    return NULL;
}

static bool within(const char *path, const char *directory) {
    size_t length = strlen(directory);
    return !strncmp(path, directory, length) && (path[length] == '/' || path[length] == '\0');
}

static int private_parent(const char *path, char leaf[NAME_MAX + 1]) {
    int parent = cj_absolute_parent(path, leaf);
    if (parent < 0) return parent;
    struct stat attributes;
    if (fstat(parent, &attributes) != 0 || attributes.st_uid != 0 || (attributes.st_mode & 0022)) {
        close(parent); return -EACCES;
    }
    return parent;
}

static int control_client(const char *path, const char *command) {
    if (strlen(path) >= sizeof(((struct sockaddr_un *) 0)->sun_path) || strlen(command) > CF_COMMAND_MAX) return 2;
    for (const unsigned char *p = (const unsigned char *) command; *p; ++p) if (*p < 0x20 || *p >= 0x7f) return 2;
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return 2;
    struct sockaddr_un address = {.sun_family = AF_UNIX}; strcpy(address.sun_path, path);
    uint64_t deadline = cf_monotonic_ns(NULL) + UINT64_C(3000000000);
    int connected = connect(fd, (struct sockaddr *) &address, sizeof(address));
    if (connected != 0 && errno != EINPROGRESS && errno != EAGAIN) { perror("control connect"); close(fd); return 2; }
    char request[CF_COMMAND_MAX + 2]; snprintf(request, sizeof(request), "%s\n", command);
    char response[CF_RESPONSE_MAX]; size_t sent = 0, received = 0, length = strlen(request);
    for (;;) {
        uint64_t now = cf_monotonic_ns(NULL);
        if (now >= deadline) break;
        uint64_t remaining = deadline - now;
        struct pollfd poller = {.fd = fd, .events = sent < length ? POLLOUT : POLLIN};
        int selected = poll(&poller, 1, (int) ((remaining + 999999) / 1000000));
        if (selected < 0 && errno == EINTR) continue;
        if (selected <= 0 || (poller.revents & (POLLERR | POLLNVAL))) break;
        if (sent < length) {
            ssize_t count = send(fd, request + sent, length - sent, MSG_NOSIGNAL);
            if (count < 0 && (errno == EAGAIN || errno == EINTR)) continue;
            if (count <= 0) break;
            sent += (size_t) count;
        } else {
            ssize_t count = recv(fd, response + received, sizeof(response) - 1 - received, 0);
            if (count < 0 && (errno == EAGAIN || errno == EINTR)) continue;
            if (count <= 0) break;
            received += (size_t) count; response[received] = '\0';
            if (memchr(response, '\n', received)) {
                fputs(response, stdout); close(fd); return strstr(response, "\"ok\":true") ? 0 : 1;
            }
            if (received == sizeof(response) - 1) break;
        }
    }
    fputs("Bounded control exchange failed or timed out\n", stderr); close(fd); return 2;
}

int main(int argc, char **argv) {
    if (argc == 4 && !strcmp(argv[1], "--ctl")) return control_client(argv[2], argv[3]);
    if (argc == 2 && !strcmp(argv[1], "--version")) { printf("checkpoint-fuse-v1 libfuse=%s\n", fuse_pkgversion()); return 0; }
    const char *backing = NULL, *mountpoint = NULL, *control = NULL, *journal_path = NULL;
    const char *attempt = NULL, *storage = NULL, *instance = NULL, *namespace_name = NULL;
    uint64_t uid = UINT64_MAX, gid = UINT64_MAX, max_bytes = 8 * 1024 * 1024, max_records = 10000;
    for (int index = 1; index < argc; index += 2) {
        if (index + 1 >= argc) { fputs("All daemon options require a value\n", stderr); return 2; }
        const char *option = argv[index], *value = argv[index + 1];
        if (!strcmp(option, "--backing")) backing = value;
        else if (!strcmp(option, "--mount")) mountpoint = value;
        else if (!strcmp(option, "--control")) control = value;
        else if (!strcmp(option, "--journal")) journal_path = value;
        else if (!strcmp(option, "--attempt")) attempt = value;
        else if (!strcmp(option, "--storage")) storage = value;
        else if (!strcmp(option, "--instance")) instance = value;
        else if (!strcmp(option, "--namespace")) namespace_name = value;
        else if (!strcmp(option, "--uid")) { if (!cf_uint(value, &uid)) return 2; }
        else if (!strcmp(option, "--gid")) { if (!cf_uint(value, &gid)) return 2; }
        else if (!strcmp(option, "--max-journal-bytes")) { if (!cf_uint(value, &max_bytes)) return 2; }
        else if (!strcmp(option, "--max-journal-records")) { if (!cf_uint(value, &max_records)) return 2; }
        else { fprintf(stderr, "Unknown daemon option: %s\n", option); return 2; }
    }
    if (geteuid() != 0 || !backing || !mountpoint || !control || !journal_path || !attempt || !storage || !instance ||
        !namespace_name || uid == 0 || gid == 0 || uid > UINT_MAX || gid > UINT_MAX ||
        within(backing, mountpoint) || within(mountpoint, backing) || within(control, mountpoint) || within(journal_path, mountpoint) ||
        within(control, backing) || within(journal_path, backing) || strlen(control) >= sizeof(((struct sockaddr_un *) 0)->sun_path)) {
        fputs("Invalid identity, privilege or isolated path configuration\n", stderr); return 2;
    }
    view_uid = (unsigned int) uid; view_gid = (unsigned int) gid;
    umask(0077);
    int mount_fd = cj_absolute_directory(mountpoint);
    if (mount_fd < 0) { fputs("Mount directory unavailable\n", stderr); return 2; }
    struct stat attributes;
    if (fstat(mount_fd, &attributes) != 0 || attributes.st_uid != 0 || (attributes.st_mode & 0022)) { close(mount_fd); return 2; }
    DIR *mount_entries = fdopendir(mount_fd);
    if (!mount_entries) { close(mount_fd); return 2; }
    bool empty = true; struct dirent *entry;
    while ((entry = readdir(mount_entries)) != NULL) if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..")) empty = false;
    closedir(mount_entries);
    if (!empty || cj_init(&jail, backing, namespace_name) != 0) { fputs("Backing jail or empty mount validation failed\n", stderr); return 2; }
    char journal_leaf[NAME_MAX + 1], control_leaf[NAME_MAX + 1];
    int journal_parent = private_parent(journal_path, journal_leaf), control_parent = private_parent(control, control_leaf);
    if (journal_parent < 0 || control_parent < 0) { fputs("Control/evidence parents must be private root directories\n", stderr); return 2; }
    int journal_fd = openat(journal_parent, journal_leaf, O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0600);
    close(journal_parent);
    if (journal_fd < 0 || cf_init(&state, attempt, storage, instance, namespace_name, journal_fd,
            max_bytes, max_records, NULL, NULL, NULL) != 0) { fputs("Fresh bounded journal initialization failed\n", stderr); return 2; }
    listener = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_un address = {.sun_family = AF_UNIX}; strcpy(address.sun_path, control);
    if (listener < 0 || bind(listener, (struct sockaddr *) &address, sizeof(address)) != 0 || listen(listener, 4) != 0) {
        cf_shutdown(&state, "control-bind-failed"); fputs("Control socket initialization failed\n", stderr); return 2;
    }
    char *fuse_argv[] = {argv[0], "-o", "allow_other,default_permissions,entry_timeout=0,attr_timeout=0,negative_timeout=0", NULL};
    struct fuse_args arguments = FUSE_ARGS_INIT(3, fuse_argv);
    filesystem = fuse_new(&arguments, &operations, sizeof(operations), &state);
    if (!filesystem || fuse_mount(filesystem, mountpoint) != 0) {
        cf_shutdown(&state, "mount-unavailable"); fputs("FUSE mount unavailable\n", stderr);
        if (filesystem) fuse_destroy(filesystem);
        close(listener); unlinkat(control_parent, control_leaf, 0); close(control_parent); close(journal_fd); cj_destroy(&jail);
        cf_destroy(&state); fuse_opt_free_args(&arguments); return 2;
    }
    atomic_store(&mounted, true);
    if (fuse_set_signal_handlers(fuse_get_session(filesystem)) != 0) { cf_shutdown(&state, "signal-handlers-unavailable"); unmount_once(); return 2; }
    pthread_t controller;
    int thread_error = pthread_create(&controller, NULL, control_loop, NULL);
    if (thread_error) { cf_shutdown(&state, "control-thread-unavailable"); unmount_once(); return 2; }
    int loop_result = fuse_loop(filesystem);
    cf_shutdown(&state, loop_result ? "fuse-loop-failed" : "loop-ended-without-stop");
    pthread_join(controller, NULL);
    unmount_once(); fuse_remove_signal_handlers(fuse_get_session(filesystem)); fuse_destroy(filesystem);
    close(listener); unlinkat(control_parent, control_leaf, 0); close(control_parent);
    bool success = state.journal_complete && state.mode == CF_STOPPED && loop_result == 0;
    close(journal_fd); cf_destroy(&state); cj_destroy(&jail); fuse_opt_free_args(&arguments);
    return success ? 0 : 1;
}
