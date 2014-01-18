/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "logw.h"
#include "buf.h"
#include "file.h"
#include "xmalloc.h"

/**
 * @file logw.c
 * @brief redo-log writer(thread-unsafe)
 *
 */

struct logw *logw_open(struct options *opts, uint64_t logsn)
{
	int flag;
	mode_t mode;
	struct logw *lgw;
	char name[FILE_NAME_MAXLEN];

	mode = S_IRWXU|S_IRWXG|S_IRWXO;
	flag = O_CREAT | O_WRONLY | O_BINARY;

	ness_check_dir(opts->redo_path);
	memset(name, 0, FILE_NAME_MAXLEN);
	snprintf(name, FILE_NAME_MAXLEN, "%s/ness.redo.%" PRIu64,
			opts->redo_path, logsn);
	lgw = xcalloc(1, sizeof(*lgw));
	lgw->base_size = (1 << 20);
	lgw->base = xcalloc(lgw->base_size, sizeof(char*));
	lgw->fd = ness_os_open(name, flag, mode);
	nassert(lgw->fd > -1);

	return lgw;
}

/*
 * REQUIRES:
 *	in_buf hold the lock
 */
static inline void _check_space(struct logw *lgw, uint32_t bytes_need)
{
	if (bytes_need > lgw->base_size) {
		uint32_t new_size;
		uint32_t double_size;

		double_size = 2 * lgw->base_size;
		new_size = bytes_need > double_size ? bytes_need : double_size;
		lgw->base = (char*)xrealloc(lgw->base, new_size);
		lgw->base_size = new_size;
	}
}

int logw_append(struct logw *lgw,
		struct msg *k,
		struct msg *v,
		msgtype_t t,
		int tbn)
{
	int r = NESS_OK;
	char *base;
	uint32_t pos = 0;
	uint32_t size = + 4	/* length of beginning */
		+ 4		/* table number */
		+ CRC_SIZE;

	size += sizeof(k->size);
	size += k->size;
	if (v) {
		size += sizeof(v->size);;
		size += v->size;
	}

	_check_space(lgw, size);
	base = lgw->base;
	putuint32(base + pos, size);
	pos += 4;

	putuint32(base + pos, tbn);
	pos += 4;

	uint32_t fixsize = k->size;

	fixsize = ((fixsize << 8) | (char)t);
	putuint32(base + pos, fixsize);
	pos += 4;
	putnstr(base + pos, k->data, k->size);
	pos += k->size;

	if (v) {
		putuint32(base + pos, v->size);
		pos += 4;
		putnstr(base + pos, v->data, v->size);
		pos += v->size;
	}

	uint32_t xsum;

	/* not include the length of the header */
	r = buf_xsum(base + 4, pos - 4, &xsum);
	if (r == 0)
		r = NESS_DO_XSUM_ERR;

	putuint32(base + pos, xsum);
	pos += 4;

	nassert(pos == size);
	lgw->size += size;

	ness_os_write(lgw->fd, base, size);

	return r;
}

void logw_fsync(struct logw *lgw)
{
	ness_os_fsync(lgw->fd);
}

void logw_close(struct logw *lgw)
{
	ness_os_fsync(lgw->fd);
	ness_os_close(lgw->fd);
	xfree(lgw->base);
	xfree(lgw);
}
