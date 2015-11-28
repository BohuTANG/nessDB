/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_LOGR_H_
#define nessDB_LOGR_H_

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


