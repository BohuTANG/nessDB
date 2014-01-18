/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LOGW_H_
#define nessDB_LOGW_H_

#include "internal.h"
#include "options.h"
#include "posix.h"
#include "msg.h"

struct logw {
	int fd;
	char *base;
	uint32_t base_size;
	uint64_t size;
	struct options *opts;
};

struct logw *logw_open(struct options *opts, uint64_t logsn);

int logw_append(struct logw *lgw,
		struct msg *k,
		struct msg *v,
		msgtype_t t,
		int tbn);

void logw_close(struct logw *lgw);

#endif /* nessDB_LOGWRITER_H_ */
