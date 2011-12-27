/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
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
#include <pthread.h>

#include "sst.h"
#include "index.h"
#include "skiplist.h"
#include "log.h"
#include "buffer.h"
#include "debug.h"

#define DB_DIR "ndbs"

static volatile int _ismerge = 0;
static volatile int _stopjob = 0;
static pthread_t _bgmerge;
static pthread_mutex_t _idx_mutex;

void *_merge_job(void *arg)
{
	volatile int lsn = 0;
	struct m_list *ml = NULL;
	struct skiplist *list = NULL;
	struct index *idx =(struct index*)arg;
	while (_stopjob == 0) {
		ml = idx->head;
		lsn = ml->lsn;
		if (ml && ml->stable) {
			_ismerge = 1;
			__DEBUG("---->>>>> Background merge start, merge count:<%d>", ml->list->count);
			list = ml->list;
			sst_merge(idx->sst, list);
			skiplist_free(list);

			pthread_mutex_lock(&_idx_mutex);
			idx->head = ml->nxt;
			idx->head->pre = NULL;
			pthread_mutex_unlock(&_idx_mutex);

			/* Remove the log which has been merged */
			log_remove(idx->log, lsn);

			/* TODO: need to free ml */
			idx->queue--;
			_ismerge = 0;
			__DEBUG("---->>>>> Back merge end, waiting merge queue count:<%d>", idx->queue);
		} else
			sleep(5);
	}
	return NULL;
}

struct index *index_new(const char *basedir, const char *name, int max_mtbl_size, int tolog)
{
	char dir[INDEX_NSIZE];
	char dbfile[DB_NSIZE];
	struct index *idx = malloc(sizeof(struct index));

	memset(dir, 0, INDEX_NSIZE);
	snprintf(dir, INDEX_NSIZE, "%s/%s", basedir, DB_DIR);
	_ensure_dir_exists(dir);
	
	idx->lsn = 0;
	idx->queue = 0;
	idx->max_mtbl = 1;
	idx->max_mtbl_size = max_mtbl_size;
	memset(idx->basedir, 0, INDEX_NSIZE);
	memcpy(idx->basedir, dir, INDEX_NSIZE);

	memset(idx->name, 0, INDEX_NSIZE);
	memcpy(idx->name, name, INDEX_NSIZE); /* mtable */
	idx->head = calloc(1, sizeof(struct m_list));
	idx->head->lsn = idx->lsn;
	idx->head->stable = 0;
	idx->head->list = skiplist_new(max_mtbl_size);
	idx->head->pre = NULL;
	idx->head->nxt = NULL;

	idx->last = idx->head;

	/* sst */
	idx->sst = sst_new(idx->basedir);

	/* log */
	idx->log = log_new(idx->basedir, idx->lsn, tolog);
	log_recovery(idx->log, idx->head->list);

	memset(dbfile, 0, DB_NSIZE);
	snprintf(dbfile, DB_NSIZE, "%s/%s.db", idx->basedir, name);
	idx->db_rfd = open(dbfile, LSM_OPEN_FLAGS, 0644);

	if (pthread_create(&_bgmerge, NULL, _merge_job, idx) != 0) {
		perror("Can't create background merge thread!");
	}

	return idx;
}

int index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	uint64_t db_offset;
	struct skiplist *list;

	db_offset = log_append(idx->log, sk, sv);
	list = idx->last->list;

	if (!list) {
		__DEBUG("ERROR: List<%d> is NULL", idx->lsn);
		return 0;
	}

	if (!skiplist_notfull(list)) {
		struct m_list *ml = malloc(sizeof(struct m_list));

		ml->stable = 0;
		idx->last->stable = 1;
		list = skiplist_new(idx->max_mtbl_size);
		ml->list = list;

		pthread_mutex_lock(&_idx_mutex);
		idx->last->nxt = ml;
		ml->pre = idx->last;
		ml->nxt = NULL;

		idx->last = ml;	
		pthread_mutex_unlock(&_idx_mutex);

		idx->queue++;
		idx->lsn++;
		ml->lsn = idx->lsn;
		log_next(idx->log, ml->lsn);
	}
	skiplist_insert(list, sk->data, db_offset, sv == NULL?DEL:ADD);

	return 1;
}

void _index_flush(struct index *idx)
{
	/* Stop background job */
	struct m_list *ml;
	struct skiplist *list;

	_stopjob = 1;
	__DEBUG("%s", "Waiting for flushing the reset of indixes to disk....");
	while (_ismerge == 1);

	ml = idx->head;
	while (ml != NULL) {
		list = ml->list;
		sst_merge(idx->sst, list);
		idx->queue--;
		skiplist_free(list);
		ml = ml->nxt;
	}
}

char *index_get(struct index *idx, struct slice *sk)
{
	int vlen = 0;
	int result = 0;
	uint64_t off = 0UL;

	struct skipnode *node = NULL;
	struct m_list *ml = idx->last;
	struct skiplist *list = NULL;

	while (ml != NULL) {
		list = ml->list;
		node = skiplist_lookup(list, sk->data);
		if (node)
			break;
		ml = ml->pre;
	}
	if (node && node->opt == ADD)
		off = node->val;
	else { 
		/* TODO: lock */
		off = sst_getoff(idx->sst, sk);
	}

	if (off != 0) {
		__be32 blen;
		lseek(idx->db_rfd, off, SEEK_SET);
		result = read(idx->db_rfd, &blen, sizeof(int));
		if(FILE_ERR(result)) 
			goto out_get;

		vlen = from_be32(blen);
		if(result == sizeof(int)) {
			char *data = malloc(vlen + 1);
			memset(data, 0, vlen+1);
			result = read(idx->db_rfd, data, vlen);
			if(FILE_ERR(result)) {
				free(data);
				goto out_get;
			}
			return data;
		}
	}

out_get:
	return NULL;
}

void index_free(struct index *idx)
{
	close(idx->db_rfd);
	_index_flush(idx);
	log_free(idx->log);
	free(idx);
}
