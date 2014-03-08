/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LOGR_H_
#define nessDB_LOGR_H_

#include "internal.h"
#include "xtypes.h"
#include "options.h"
#include "msg.h"

struct logr {
	int fd;
	uint32_t fsize;
	uint32_t read;
	uint32_t base_size;
	char *base;
};

struct logr *logr_open(struct options *opts, uint64_t logsn);
int logr_read(struct logr *lgr,
		struct msg *k,
		struct msg *v,
		msgtype_t *t,
		uint32_t *tbn);
void logr_close(struct logr *lgr);

#endif /* nessDB_LOGR_H_ */


