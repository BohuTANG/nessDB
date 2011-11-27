#ifndef _LOG_H
#define _LOG_H

#include "util.h"

#define LOG_NSIZE (256)

struct log{
	int fd;
	char name[LOG_NSIZE];
	struct buffer *buf;
};

struct log *log_new(char *name);
void log_append(struct log *log, struct slice *sk, uint64_t offset, uint8_t opt);
void log_trunc(struct log *log);
void log_free(struct log *log);

#endif
