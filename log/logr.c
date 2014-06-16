/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "logr.h"
#include "file.h"
#include "msgpack.h"
#include "debug.h"

#define LOG_BASE_SIZE (4194304) /* 4MB */

size_t _file_size(char *filename)
{
	struct stat st;
	stat(filename, &st);

	return st.st_size;
}

struct logr *logr_open(struct options *opts, uint64_t logsn) {
	int flag;
	mode_t mode;
	struct logr *lgr;
	char name[FILE_NAME_MAXLEN];

	mode = S_IRWXU | S_IRWXG | S_IRWXO;
	flag = O_RDONLY | O_BINARY;

	memset(name, 0, FILE_NAME_MAXLEN);
	snprintf(name, FILE_NAME_MAXLEN, "%s/ness.redo.%" PRIu64,
	         opts->redo_path, logsn);
	lgr = xcalloc(1, sizeof(*lgr));
	lgr->fd = ness_os_open(name, flag, mode);
	lgr->fsize = _file_size(name);
	lgr->base = xmalloc(LOG_BASE_SIZE);
	lgr->base_size = LOG_BASE_SIZE;

	return lgr;
}

int logr_read(struct logr *lgr,
              struct msg *k,
              struct msg *v,
              msgtype_t *t,
              uint32_t *tbn)
{
	int r;
	uint32_t size = 0U;

	if (lgr->read < lgr->fsize) {
		char *base;
		uint32_t read;

		if (ness_os_read(lgr->fd, lgr->base, 4) != 4) {
			r = NESS_LOG_READ_SIZE_ERR;
			__ERROR(" log read size error, errno [%d]", r);
			goto ERR;
		}

		getuint32(lgr->base, &size);

		if (size > lgr->base_size) {
			lgr->base_size = size;
			lgr->base = xrealloc(lgr->base, lgr->base_size);
		}

		/* skip the length */
		read = (size - 4);
		if (ness_os_read(lgr->fd, lgr->base, read) != read) {
			r = NESS_LOG_READ_DATA_ERR;
			__ERROR(" log read entry error, errno [%d]", r);
			goto ERR;
		}

		base = lgr->base;

		uint32_t exp_xsum;
		uint32_t act_xsum;

		getuint32(base + read - 4, &exp_xsum);
		buf_xsum(base, read - 4, &act_xsum);

		if (exp_xsum != act_xsum) {
			r = NESS_LOG_READ_XSUM_ERR;
			__ERROR("log read xsum error, exp_xsum [%" PRIu32 "],act_xsum [%" PRIu32 "]",
			        exp_xsum,
			        act_xsum);
			goto ERR;
		}

		int pos = 0;

		/* tbn */
		getuint32(base + pos, tbn);
		pos += 4;

		/* key */
		uint32_t fixsize;

		getuint32(base + pos, &fixsize);
		pos += 4;
		k->size = (fixsize >> 8);
		*t = (msgtype_t)(fixsize & 0xff);

		getnstr(base + pos, k->size, (char**)&k->data);
		pos += k->size;

		if (*t != MSG_DELETE) {
			/* value */
			getuint32(base + pos, &v->size);
			pos += 4;
			getnstr(base + pos, v->size, (char**)&v->data);
			pos += v->size;
		}
	} else {
		r = NESS_LOG_EOF;
		goto ERR;
	}

	lgr->read += size;

	return NESS_OK;

ERR:
	return r;
}

void logr_close(struct logr *lgr)
{
	xfree(lgr->base);
	ness_os_close(lgr->fd);
	xfree(lgr);
}
