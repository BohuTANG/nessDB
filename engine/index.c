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

#include "sst.h"
#include "index.h"
#include "log.h"
#include "buffer.h"
#include "debug.h"

#define DB_DIR "ndbs"

void *_merge_job(void *arg)
{
	int lsn;
	struct index *idx;
	struct skiplist *list;
	struct sst *sst;
	struct log *log;

	/* Lock begin */
	idx = (struct index*)arg;
	lsn = idx->park->lsn;
	list = idx->park->list;
	sst = idx->sst;
	log = idx->log;

	if(list == NULL)
		goto out;

	pthread_mutex_lock(&idx->merge_mutex);
	sst_merge(sst, list);
	pthread_mutex_unlock(&idx->merge_mutex);

	/* Lock end */
	log_remove(log, lsn);

out:
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

struct index *index_new(const char *basedir, const char *name, int max_mtbl_size, int tolog)
{
	char dir[INDEX_NSIZE];
	char dbfile[DB_NSIZE];
	struct index *idx = malloc(sizeof(struct index));
	struct idx_park *park = malloc(sizeof(struct idx_park));

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

	/* sst */
	idx->sst = sst_new(idx->basedir);
	idx->list = skiplist_new(max_mtbl_size);

	/* container */
	park->list = NULL; 	
	park->lsn = idx->lsn;
	idx->park = park;

	/* log */
	idx->log = log_new(idx->basedir, idx->lsn, tolog);
	log_recovery(idx->log, idx->list);

	memset(dbfile, 0, DB_NSIZE);
	snprintf(dbfile, DB_NSIZE, "%s/%s.db", idx->basedir, name);
	idx->db_rfd = open(dbfile, LSM_OPEN_FLAGS, 0644);

	/* Detached thread attr */
	pthread_attr_init(&idx->attr);
	pthread_attr_setdetachstate(&idx->attr, PTHREAD_CREATE_DETACHED);


	return idx;
}

int index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	uint64_t value_offset;
	struct skiplist *list, *new_list;

	value_offset = log_append(idx->log, sk, sv);
	list = idx->list;

	if (!list) {
		__DEBUG("ERROR: List<%d> is NULL", idx->lsn);
		return 0;
	}

	if (!skiplist_notfull(list)) {

		/* If the detached-merge thread isnot finished,hold on it 
		 * Notice: it will block the current process, 
		 * but it happens only once in a thousand years on production environment.
		*/
		pthread_mutex_lock(&idx->merge_mutex);

		/* Start to merge with detached thread */
		pthread_t tid;
		idx->park->list = list;
		idx->park->lsn = idx->lsn;
		pthread_mutex_unlock(&idx->merge_mutex);

		pthread_create(&tid, &idx->attr, _merge_job, idx);

		/* New mtable is born */
		new_list = skiplist_new(idx->max_mtbl_size);
		idx->list = new_list;

		idx->lsn++;
		log_next(idx->log, idx->lsn);
	}
	skiplist_insert(idx->list, sk->data, value_offset, sv == NULL?DEL:ADD);

	return 1;
}

void _index_flush(struct index *idx)
{
	struct skiplist *list;

	pthread_mutex_lock(&idx->merge_mutex);
	pthread_mutex_unlock(&idx->merge_mutex);

	list = idx->list;
	if (!list) 
		return;

	sst_merge(idx->sst, list);
	log_remove(idx->log, idx->lsn);
}

/*
 * Return status: -1:error 0:NULL 1:exists
 */
int index_get(struct index *idx, struct slice *sk, struct slice *sv)
{
	int ret = 0, value_len, result;
	uint64_t value_off = 0UL;

	struct skipnode *node;
	struct skiplist *cur_list;
	struct skiplist *merge_list;

	/* 1)First lookup from active memtable
 	 * 2)Then from merge memtable 
 	 * 3)Last from sst on-disk indexes
 	 */
	cur_list = idx->list;
	node = skiplist_lookup(cur_list, sk->data);
	if (node){
		if(node->opt == DEL) {
			ret  = -1;
			goto out_get;
		}
		value_off = node->val;
	} else {
		merge_list = idx->park->list;
		if (merge_list) {
			node = skiplist_lookup(merge_list, sk->data);
			if (node && node->opt == ADD )
				value_off = node->val;
		}
	}

	if (value_off == 0UL)
		value_off = sst_getoff(idx->sst, sk);

	if (value_off != 0UL) {
		__be32 be32len;
		lseek(idx->db_rfd, value_off, SEEK_SET);
		result = read(idx->db_rfd, &be32len, sizeof(int));
		if(FILE_ERR(result)) {
			ret = -1;
			goto out_get;
		}

		value_len = from_be32(be32len);
		if(result == sizeof(int)) {
			char *data = malloc(value_len + 1);
			memset(data, 0, value_len + 1);
			result = read(idx->db_rfd, data, value_len);
			if(FILE_ERR(result)) {
				free(data);
				ret = -1;
				goto out_get;
			}
			sv->len = value_len;
			sv->data = data;
			return 1;
		}
	}

out_get:
	return ret;
}

void index_free(struct index *idx)
{
	_index_flush(idx);
	pthread_attr_destroy(&idx->attr);
	log_free(idx->log);
	close(idx->db_rfd);
	free(idx->park);
	free(idx);
}
