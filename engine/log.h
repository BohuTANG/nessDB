#ifndef __nessDB_LOG_H
#define __nessDB_LOG_H

#include "db.h"
#include "meta.h"
#include "buffer.h"

struct log {
	int fd;
	int db_fd;
	int no;
	int islog;
	char path[NESSDB_PATH_SIZE];
	char file[NESSDB_PATH_SIZE];
	struct buffer *buf;
};

struct log *log_new(const char *path, struct meta *meta, int islog);
void log_append(struct log *log, struct cola_item *item);
void log_remove(struct log *log, int logno);
void log_create(struct log *log);
void log_free(struct log *log);

#endif
