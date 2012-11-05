#ifndef _LOG_H
#define _LOG_H

#include "config.h"
#include "buffer.h"

struct kv_pair {
	struct slice sk;
	struct slice sv;
	struct kv_pair *nxt;
};

struct log {
	int fd;
	int no;
	int islog;
	char path[NESSDB_PATH_SIZE];
	char file[NESSDB_PATH_SIZE];
	struct buffer *buf;
	struct kv_pair *redo;
};

struct log *log_new(const char *path, int islog);
void log_append(struct log *log, struct slice *sk, struct slice *sv);
void log_create(struct log *log);
void log_remove(struct log *log);
void log_free(struct log *log);

#endif
