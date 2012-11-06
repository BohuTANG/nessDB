/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "config.h"

#include "meta.h"
#include "cola.h"
#include "log.h"
#include "debug.h"
#include "xmalloc.h"

void _make_log_name(struct log *log, int lsn)
{
	memset(log->file, 0, NESSDB_PATH_SIZE);
	snprintf(log->file, NESSDB_PATH_SIZE, "%s/%06d%s", log->path, lsn, NESSDB_LOG_EXT);
}

int  _find_maxno_log(struct log *log)
{
	int lsn;
	int max = 0;
	DIR *dd;
	struct dirent *de;
	char name[NESSDB_PATH_SIZE];

	dd = opendir(log->path);
	while ((de = readdir(dd))) {
		if (strstr(de->d_name, NESSDB_LOG_EXT)) {
			memset(name, 0, NESSDB_PATH_SIZE);
			memcpy(name, de->d_name, strlen(de->d_name) - 4);
			lsn = atoi(name);
			max = lsn > max ? lsn : max;
		}
	}
	closedir(dd);

	return max;
}

void log_create(struct log *log)
{
	log->no++;
	_make_log_name(log, log->no);
	__DEBUG("create new log#%s", log->file);

	if (log->fd > 0)
		close(log->fd);

	log->fd = n_open(log->file, N_CREAT_FLAGS, 0644);
	if (log->fd == -1)
		__PANIC("create log file %s....", log->file);
}

void log_remove(struct log *log, int logno)
{
	int res;

	_make_log_name(log, logno);
	__DEBUG("remove log#%s", log->file);

	res = remove(log->file);
	if (res == -1)
		__ERROR("remove log %s error", log->file);
}

void _log_read(struct log *log, struct meta *meta, int no)
{
	int fd;
	int sizes;
	int rem;;
	struct meta_node *node;
	struct cola_item itm;

	_make_log_name(log, no);
	fd = n_open(log->file, N_OPEN_FLAGS, 0644);

	if (fd == -1) {
		__ERROR("read log error, %s", log->file);
		return;
	}

	rem = sizes = lseek(fd, 0, SEEK_END);

	__DEBUG("--->begin to recover log#%s, log-sizes#%d", log->file, sizes);
	lseek(fd, 0, SEEK_SET);
	while (rem) {
		int klen = 0;
		int vlen = 0;
		short opt = 0;
		uint64_t v = 0;

		memset(&itm, 0, ITEM_SIZE);

		if (read(fd, &klen, sizeof klen) != sizeof klen)
			__PANIC("read klen error");
		rem -= sizeof klen;

		if (read(fd, itm.data, klen) != klen)
			__PANIC("error when read key");
		rem -= klen;

		if (read(fd, &vlen, sizeof vlen) != sizeof vlen)
			__PANIC("error when read vlen");
		rem -= sizeof vlen;
		itm.vlen = vlen;

		if (read(fd, &v, sizeof v) != sizeof v)
			__PANIC("read v error");
		rem -= sizeof v;
		itm.offset = v;

		if (read(fd, &opt, sizeof opt) != sizeof opt)
			__PANIC("error when read opt");
		rem -= sizeof opt;
		itm.opt = opt;

		node = meta_get(meta, itm.data);
		cola_add(node->cola, &itm);
	}

	log_remove(log, no);
	if (fd > 0)
		close(fd);
}

void _log_recovery(struct log *log, struct meta *meta)
{
	int i;

	if (log->no < 1 || !log->islog)
		return;

	for (i = log->no - 1; i <= log->no; i++)
		_log_read(log, meta, i);
}

struct log *log_new(const char *path, struct meta *meta, int islog)
{
	int max;
	struct log *log;
	
	log = xcalloc(1, sizeof(struct log));
	memcpy(log->path, path, strlen(path));
	log->buf = buffer_new(1024 * 1024 *16); /* 10MB buffer*/

	max = _find_maxno_log(log);
	log->no = max;
	log->islog = islog;

	_log_recovery(log, meta);
	log_create(log);

	return log;
}

void log_append(struct log *log, struct cola_item *itm)
{
	int res;
	int len;
	int klen;
	char *block;

	if (!log->islog)
		return;

	klen = strlen(itm->data);
	buffer_putint(log->buf, klen);
	buffer_putnstr(log->buf, itm->data, klen);
	buffer_putint(log->buf, itm->vlen);
	buffer_putlong(log->buf, itm->offset);
	buffer_putshort(log->buf, itm->opt);

	len = log->buf->NUL;
	block = buffer_detach(log->buf);

	res = write(log->fd, block, len);
	if (res == -1) {
		__ERROR("log error!!!");
	}
}

void log_free(struct log *log)
{
	if (log->fd > 0)
		close(log->fd);

	buffer_free(log->buf);
	free(log);
}
