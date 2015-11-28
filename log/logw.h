/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_LOGW_H_
#define nessDB_LOGW_H_

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
