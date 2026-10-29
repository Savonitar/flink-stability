#ifndef CHECKPOINT_FUSE_JAIL_H
#define CHECKPOINT_FUSE_JAIL_H

#include "core.h"
#include <limits.h>
#include <sys/stat.h>

struct cj_jail { int backing_fd, namespace_fd; char namespace_name[CF_ID_MAX + 1]; };

int cj_absolute_directory(const char *path);
int cj_absolute_parent(const char *path, char leaf[NAME_MAX + 1]);
int cj_init(struct cj_jail *jail, const char *backing, const char *namespace_name);
void cj_destroy(struct cj_jail *jail);
int cj_validate(const struct cj_jail *jail, const char *path);
/* Caller holds state->mutex. Invalid paths never receive the namespace fault. */
int cj_begin_locked(const struct cj_jail *jail, struct cf_state *state, const char *path, const char *fault_operation);
int cj_begin_operation_locked(const struct cj_jail *jail, struct cf_state *state, const char *path,
                              const char *fault_operation, bool mutation);
bool cj_namespace_root(const struct cj_jail *jail, const char *path);
int cj_parent(const struct cj_jail *jail, const char *path, char leaf[NAME_MAX + 1]);
int cj_lstat(const struct cj_jail *jail, const char *path, struct stat *attributes);
int cj_open(const struct cj_jail *jail, const char *path, int flags, mode_t mode);
int cj_type(const struct stat *attributes);

#endif
