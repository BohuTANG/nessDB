/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef __USE_FILE_OFFSET64
	#define __USE_FILE_OFFSET64
#endif

#ifndef __USE_LARGEFILE64
	#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE64_SOURCE
	#define _LARGEFILE64_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>

#include "buffer.h"
#include "log.h"
#include "debug.h"

#define DB_MAGIC (2011)

int _file_exists(const char *path)
{
	int fd = open(path, O_RDWR);
	if (fd > -1) {
		close(fd);
		return 1;
	}
	return 0;
}

struct log *log_new(const char *basedir, int lsn, int islog)
{
	int result;
	struct log *l;
	char log_name[LOG_NSIZE];
	char db_name[LOG_NSIZE];

	l = malloc(sizeof(struct log));
	l->islog = islog;

	memset(log_name, 0, LOG_NSIZE);
	snprintf(log_name, LOG_NSIZE, "%s/%d.log", basedir, 0);
	memcpy(l->name, log_name, LOG_NSIZE);

	memset(l->basedir, 0, LOG_NSIZE);
	memcpy(l->basedir, basedir, LOG_NSIZE);

	memset(db_name, 0, LOG_NSIZE);
	snprintf(db_name, LOG_NSIZE, "%s/ness.db", basedir);


	if (_file_exists(db_name)) {
		l->db_wfd = open(db_name, LSM_OPEN_FLAGS, 0644);
		l->db_alloc = lseek(l->db_wfd, 0, SEEK_END);
	} else {
		int magic = DB_MAGIC;

		l->db_wfd = open(db_name, LSM_CREAT_FLAGS, 0644);
		result = write(l->db_wfd, &magic, sizeof(int));
		if (result == -1) 
			perror("write magic error\n");
		l->db_alloc = sizeof(int);
	}

	l->buf = buffer_new(256);
	l->db_buf = buffer_new(1024);

	return l;
}

void _log_read(char *logname, struct skiplist *list)
{
	int rem;
	int fd = open(logname, O_RDWR, 0644);
	int size = lseek(fd, 0, SEEK_END);

	if (fd == -1)
		return;

	rem = size;
	if (size == 0)
		return;

	lseek(fd, 0, SEEK_SET);
	while(rem > 0) {
		int isize = 0;
		int klen, opt;
		uint64_t off;
		char key[SKIP_KSIZE];
		char klenstr[4], offstr[8], optstr[4];

		memset(klenstr, 0, 4);
		memset(offstr, 0, 8);
		memset(optstr, 0, 4);
		
		/* read key length */
		read(fd, &klenstr, sizeof(int));
		klen = u32_from_big((unsigned char*)klenstr);
		isize += sizeof(int);
		
		/* read key */
		memset(key, 0, SKIP_KSIZE);
		read(fd, &key, klen);
		isize += klen;

		/* read data offset */
		read(fd, &offstr, sizeof(uint64_t));
		off = u64_from_big((unsigned char*)offstr);
		isize += sizeof(uint64_t);

		/* read opteration */
		read(fd, &optstr, sizeof(int));
		opt = u32_from_big((unsigned char*)optstr);
		isize += sizeof(int);

		skiplist_insert(list, key, off, opt == 0? ADD : DEL);

		rem -= isize;
	}
}

int log_recovery(struct log *l, struct skiplist *list)
{
	DIR *dd;
	int ret = 0;
	char new_log[LOG_NSIZE];
	char old_log[LOG_NSIZE];
	struct dirent *de;

	if (!l->islog)
		return 0;

	memset(new_log, 0, LOG_NSIZE);
	memset(old_log, 0, LOG_NSIZE);

	dd = opendir(l->basedir);
	while ((de = readdir(dd))) {
		char *p = strstr(de->d_name, ".log");
		if (p) {
			if (ret == 0) 
				memcpy(new_log, de->d_name, LOG_NSIZE);
			else 
				memcpy(old_log, de->d_name, LOG_NSIZE);
			ret = 1;
		}
	}
	closedir(dd);

	/* 
	 * Get the two log files:new and old 
	 * Read must be sequential,read old then read new
	 */
	if (ret) {
		memset(l->log_old, 0, LOG_NSIZE);
		snprintf(l->log_old, LOG_NSIZE, "%s/%s", l->basedir, old_log);
		_log_read(l->log_old, list);

		memset(l->log_new, 0, LOG_NSIZE);
		snprintf(l->log_new, LOG_NSIZE, "%s/%s", l->basedir, new_log);
		_log_read(l->log_new, list);
	}

	return ret;
}

uint64_t log_append(struct log *l, struct slice *sk, struct slice *sv)
{
	int len;
	int db_len;
	char *line;
	char *db_line;
	struct buffer *buf = l->buf;
	struct buffer *db_buf = l->db_buf;
	uint64_t db_offset = l->db_alloc;

	/* DB write */
	if (sv) {
		buffer_putint(db_buf, sv->len);
		buffer_putnstr(db_buf, sv->data, sv->len);
		db_len = db_buf->NUL;
		db_line = buffer_detach(db_buf);

		if (write(l->db_wfd, db_line, db_len) != db_len) {
			__DEBUG("%s:length:<%d>", "ERROR: Data AOF **ERROR**", db_len);
			return db_offset;
		}
		l->db_alloc += db_len;
	}

	/* LOG write */
	if (l->islog) {
		buffer_putint(buf, sk->len);
		buffer_putnstr(buf, sk->data, sk->len);
		buffer_putlong(buf, db_offset);
		if(sv)
			buffer_putint(buf, 0);
		else
			buffer_putint(buf, 1);

		len = buf->NUL;
		line = buffer_detach(buf);

		if (write(l->idx_wfd, line, len) != len)
			__DEBUG("%s,buffer is:%s,buffer length:<%d>", "ERROR: Log AOF **ERROR**", line, len);
	}


	return db_offset;
}

void log_remove(struct log *l, int lsn)
{
	char log_name[LOG_NSIZE];
	memset(log_name, 0 ,LOG_NSIZE);
	snprintf(log_name, LOG_NSIZE, "%s/%d.log", l->basedir, lsn);
	remove(log_name);
}

void log_next(struct log *l, int lsn)
{
	char log_name[LOG_NSIZE];
	memset(log_name, 0 ,LOG_NSIZE);
	snprintf(log_name, LOG_NSIZE, "%s/%d.log", l->basedir, lsn);
	memcpy(l->name, log_name, LOG_NSIZE);

	buffer_clear(l->buf);
	buffer_clear(l->db_buf);

	close(l->idx_wfd);
	l->idx_wfd = open(l->name, LSM_CREAT_FLAGS, 0644);
}

void log_free(struct log *l)
{
	if (l) {
		buffer_free(l->buf);
		close(l->idx_wfd);
		free(l);
	}
}
