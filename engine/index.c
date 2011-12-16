/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "sst.h"
#include "index.h"
#include "skiplist.h"
#include "log.h"
#include "debug.h"

#define TOLOG (0)
#define DB_DIR "ndbs"

struct index *index_new(const char *basedir, const char *name, int max_mtbl_size)
{
	char dir[INDEX_NSIZE];
	struct index *idx = malloc(sizeof(struct index));

	memset(dir, 0, INDEX_NSIZE);
	snprintf(dir, INDEX_NSIZE, "%s/%s", basedir, DB_DIR);
	_ensure_dir_exists(dir);
	
	idx->lsn = 0;
	idx->max_mtbl = 1;
	idx->max_mtbl_size = max_mtbl_size;
	memset(idx->basedir, 0, INDEX_NSIZE);
	memcpy(idx->basedir, dir, INDEX_NSIZE);

	memset(idx->name, 0, INDEX_NSIZE);
	memcpy(idx->name, name, INDEX_NSIZE);

	/* mtable */
	idx->mtbls = calloc(1, sizeof(struct skiplist*));
	idx->mtbls[0] = skiplist_new(idx->max_mtbl_size);

	/* sst */
	idx->sst = sst_new(idx->basedir);

	/* log */
	idx->log = log_new(idx->basedir, idx->name, TOLOG);
	log_recovery(idx->log, idx->mtbls[0]);

	return idx;
}

int index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	uint64_t db_offset;
	struct skiplist *list;

	db_offset = log_append(idx->log, sk, sv);
	list = idx->mtbls[idx->lsn];

	if (!list) {
		__DEBUG("ERROR: List<%d> is NULL", idx->lsn);
		return 0;
	}

	if (!skiplist_notfull(list)) {
		sst_merge(idx->sst, idx->mtbls[0]);
		skiplist_free(idx->mtbls[0]);

		log_trunc(idx->log);

		list = skiplist_new(idx->max_mtbl_size);
		idx->mtbls[idx->lsn] = list;
	}
	skiplist_insert(list, sk->data, db_offset, ADD);

	return 1;
}

void index_flush(struct index *idx)
{
	struct skiplist *list;

	list = idx->mtbls[idx->lsn];

	if (!list) {
		__DEBUG("ERROR: List<%d> is NULL", idx->lsn);
		return ;
	}

	sst_merge(idx->sst, idx->mtbls[0]);
	skiplist_free(idx->mtbls[0]);

	log_trunc(idx->log);

	list = skiplist_new(idx->max_mtbl_size);
	idx->mtbls[idx->lsn] = list;
}

void index_remove(struct index *idx, struct slice *sk)
{
	uint64_t db_offset;
	struct skiplist *list;

	db_offset = log_append(idx->log, sk, NULL);
	list = idx->mtbls[idx->lsn];

	if (!list) {
		__DEBUG("ERROR: List<%d> is NULL", idx->lsn);
		return;
	}

	if (!skiplist_notfull(list)) {
		sst_merge(idx->sst, idx->mtbls[0]);
		skiplist_free(idx->mtbls[0]);

		log_trunc(idx->log);

		list = skiplist_new(idx->max_mtbl_size);
		idx->mtbls[idx->lsn] = list;
	}
	skiplist_insert(list, sk->data, db_offset, DEL);
}

char *index_get(struct index *idx, struct slice *sk)
{
	int len;
	int result;
	uint64_t off;
	struct skiplist *list = idx->mtbls[idx->lsn];
	struct skipnode *node = skiplist_lookup(list, sk->data);
	if (node && node->opt == ADD)
		off = node->val;
	else 
		off = sst_getoff(idx->sst, sk);

	if (off != 0) {
		lseek(idx->log->fd_db, off, SEEK_SET);
		if(read(idx->log->fd_db, &len, sizeof(int)) == sizeof(int)) {
			char *data = malloc(len + 1);
			result = read(idx->log->fd_db, data, len);
			if (result != -1)
				return data;
		}
	}

	return NULL;
}

void index_free(struct index *idx)
{
	__DEBUG("%s:", "Warn: mtable is flush to disk");
	index_flush(idx);
	log_free(idx->log);
	free(idx);
}
