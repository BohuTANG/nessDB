#ifndef _LOG_H
#define _LOG_H

#include "config.h"
#include "buffer.h"

struct log {
	int fd;
	int no;
	char path[NESSDB_PATH_SIZE];
	char file[NESSDB_PATH_SIZE];
	struct buffer *buf;
};

struct log *log_new(const char *path);
void log_append(struct log *log, struct slice *sk, struct slice *sv);
void log_recovery(struct log *log);
void log_free(struct log *log);

#endif
