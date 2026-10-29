#define _GNU_SOURCE
#include "jail.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

static bool safe_component(const char *value, bool generated) {
    size_t length = strlen(value);
    if (!length || length > NAME_MAX || !strcmp(value, ".") || !strcmp(value, "..")) return false;
    if (!generated) return true;
    for (const unsigned char *p = (const unsigned char *) value; *p; ++p)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '-' || *p == '_' || *p == '.')) return false;
    return true;
}

static int directory_at(int parent, const char *name) {
    int fd = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    return fd >= 0 ? fd : -errno;
}

int cj_absolute_directory(const char *path) {
    if (!path || path[0] != '/' || strlen(path) >= PATH_MAX || strstr(path, "//")) return -EINVAL;
    int fd = open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -errno;
    if (!strcmp(path, "/")) return fd;
    char copy[PATH_MAX]; strcpy(copy, path + 1);
    if (copy[strlen(copy) - 1] == '/') { close(fd); return -EINVAL; }
    char *save = NULL;
    for (char *part = strtok_r(copy, "/", &save); part; part = strtok_r(NULL, "/", &save)) {
        if (!safe_component(part, false)) { close(fd); return -EINVAL; }
        int next = directory_at(fd, part);
        close(fd);
        if (next < 0) return next;
        fd = next;
    }
    return fd;
}

int cj_absolute_parent(const char *path, char leaf[NAME_MAX + 1]) {
    if (!path || path[0] != '/' || strlen(path) >= PATH_MAX) return -EINVAL;
    char copy[PATH_MAX]; strcpy(copy, path);
    char *slash = strrchr(copy, '/');
    if (!slash || !safe_component(slash + 1, false)) return -EINVAL;
    strcpy(leaf, slash + 1);
    if (slash == copy) slash[1] = '\0'; else *slash = '\0';
    return cj_absolute_directory(copy);
}

int cj_type(const struct stat *attributes) {
    if (S_ISLNK(attributes->st_mode)) return -ELOOP;
    if (!S_ISREG(attributes->st_mode) && !S_ISDIR(attributes->st_mode)) return -EACCES;
    if (S_ISREG(attributes->st_mode) && attributes->st_nlink > 1) return -EPERM;
    return 0;
}

int cj_init(struct cj_jail *jail, const char *backing, const char *namespace_name) {
    if (!cf_identifier(namespace_name)) return -EINVAL;
    jail->backing_fd = jail->namespace_fd = -1;
    int fd = cj_absolute_directory(backing);
    if (fd < 0) return fd;
    struct stat st;
    if (fstat(fd, &st) != 0) { int error = errno; close(fd); return -error; }
    if (st.st_uid != geteuid() || (st.st_mode & 0777) != 0700) { close(fd); return -EACCES; }
    if (mkdirat(fd, namespace_name, 0700) != 0 && errno != EEXIST) { int error = errno; close(fd); return -error; }
    int namespace_fd = directory_at(fd, namespace_name);
    if (namespace_fd < 0) { close(fd); return namespace_fd; }
    if (fstat(namespace_fd, &st) != 0 || st.st_uid != geteuid() || (st.st_mode & 0777) != 0700) {
        close(namespace_fd); close(fd); return -EACCES;
    }
    jail->backing_fd = fd; jail->namespace_fd = namespace_fd;
    strcpy(jail->namespace_name, namespace_name);
    return 0;
}

void cj_destroy(struct cj_jail *jail) {
    if (jail->namespace_fd >= 0) close(jail->namespace_fd);
    if (jail->backing_fd >= 0) close(jail->backing_fd);
    jail->backing_fd = jail->namespace_fd = -1;
}

bool cj_namespace_root(const struct cj_jail *jail, const char *path) {
    return path && path[0] == '/' && !strcmp(path + 1, jail->namespace_name);
}

int cj_validate(const struct cj_jail *jail, const char *path) {
    if (path && strlen(path) > CF_PATH_MAX) return -ENAMETOOLONG;
    if (!path || path[0] != '/' || strstr(path, "//")) return -EACCES;
    if (!strcmp(path, "/")) return 0;
    size_t ns = strlen(jail->namespace_name);
    if (strncmp(path + 1, jail->namespace_name, ns) || (path[1 + ns] && path[1 + ns] != '/')) return -EACCES;
    char copy[CF_PATH_MAX + 1]; strcpy(copy, path + 1);
    if (copy[strlen(copy) - 1] == '/') return -EACCES;
    char *save = NULL;
    size_t depth = 0;
    for (char *part = strtok_r(copy, "/", &save); part; part = strtok_r(NULL, "/", &save))
        if (++depth > 32 || !safe_component(part, true)) return -EACCES;
    return 0;
}

int cj_begin_locked(const struct cj_jail *jail, struct cf_state *state, const char *path, const char *fault_operation) {
    return cj_begin_operation_locked(jail, state, path, fault_operation, fault_operation != NULL);
}

int cj_begin_operation_locked(const struct cj_jail *jail, struct cf_state *state, const char *path,
                              const char *fault_operation, bool mutation) {
    int checked = cj_validate(jail, path);
    if (!checked && fault_operation && !strcmp(path, "/")) checked = -EACCES;
    int guard = cf_begin_operation_locked(state, checked ? NULL : fault_operation, mutation);
    return guard ? guard : checked;
}

int cj_parent(const struct cj_jail *jail, const char *path, char leaf[NAME_MAX + 1]) {
    int checked = cj_validate(jail, path);
    if (checked) return checked;
    if (!strcmp(path, "/") || cj_namespace_root(jail, path)) return -EACCES;
    char copy[CF_PATH_MAX + 1];
    strcpy(copy, path + strlen(jail->namespace_name) + 2);
    char *slash = strrchr(copy, '/');
    if (!slash) { strcpy(leaf, copy); int fd = fcntl(jail->namespace_fd, F_DUPFD_CLOEXEC, 0); return fd < 0 ? -errno : fd; }
    strcpy(leaf, slash + 1); *slash = '\0';
    int fd = fcntl(jail->namespace_fd, F_DUPFD_CLOEXEC, 0);
    if (fd < 0) return -errno;
    char *save = NULL;
    for (char *part = strtok_r(copy, "/", &save); part; part = strtok_r(NULL, "/", &save)) {
        int next = directory_at(fd, part); close(fd);
        if (next < 0) return next;
        fd = next;
    }
    return fd;
}

int cj_lstat(const struct cj_jail *jail, const char *path, struct stat *attributes) {
    int error = cj_validate(jail, path);
    if (error) return error;
    if (!strcmp(path, "/") || cj_namespace_root(jail, path)) {
        if (fstat(!strcmp(path, "/") ? jail->backing_fd : jail->namespace_fd, attributes) != 0) return -errno;
    } else {
        char leaf[NAME_MAX + 1]; int parent = cj_parent(jail, path, leaf);
        if (parent < 0) return parent;
        int result = fstatat(parent, leaf, attributes, AT_SYMLINK_NOFOLLOW);
        error = errno; close(parent);
        if (result < 0) return -error;
    }
    return cj_type(attributes);
}

int cj_open(const struct cj_jail *jail, const char *path, int flags, mode_t mode) {
    int error = cj_validate(jail, path);
    if (error) return error;
    if (!strcmp(path, "/")) return -EACCES;
    if (cj_namespace_root(jail, path)) {
        if (!(flags & O_DIRECTORY) || (flags & (O_CREAT | O_TRUNC)) || (flags & O_ACCMODE) != O_RDONLY) return -EACCES;
        int fd = openat(jail->namespace_fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        return fd < 0 ? -errno : fd;
    }
    char leaf[NAME_MAX + 1]; int parent = cj_parent(jail, path, leaf);
    if (parent < 0) return parent;
    struct stat attributes;
    if (fstatat(parent, leaf, &attributes, AT_SYMLINK_NOFOLLOW) == 0) {
        error = cj_type(&attributes);
        if (error) { close(parent); return error; }
    } else if (errno != ENOENT || !(flags & O_CREAT)) {
        error = errno; close(parent); return -error;
    }
    int fd = openat(parent, leaf, flags | O_NOFOLLOW | O_CLOEXEC, mode & 0777);
    error = errno; close(parent);
    if (fd < 0) return -error;
    if (fstat(fd, &attributes) != 0) { error = errno; close(fd); return -error; }
    error = cj_type(&attributes);
    if (error) { close(fd); return error; }
    return fd;
}
