/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _LOG_H
#define _LOG_H

#include <stdint.h>
#include "skiplist.h"
#include "util.h"
#include "platform.h"

struct log{
	int idx_wfd;
	int db_wfd;
	int islog;
	uint64_t db_alloc;
	char name[LOG_NSIZE];
	char basedir[LOG_NSIZE];
	struct buffer *buf;
	struct buffer *db_buf;
};

struct log *log_new(const char *basedir, int lsn, int islog);
int log_recovery(struct log *log, struct skiplist *list);
uint64_t log_append(struct log *log, struct slice *sk, struct slice *sv);
void log_remove(struct log *log, int lsn);
void log_next(struct log *log, int lsn);
void log_free(struct log *log);

#endif
