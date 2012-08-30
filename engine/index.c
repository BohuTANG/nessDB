/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include "config.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "sst.h"
#include "log.h"
#include "buffer.h"
#include "debug.h"
#include "bloom.h"
#include "index.h"

void *_merge_job(void *arg)
{
	int lsn, list_count;
	long long start, end, cost;
	struct index *idx;
	struct skiplist *list;
	struct sst *sst;
	struct log *log;

	idx = (struct index*)arg;
	lsn = idx->park->lsn;
	list = idx->park->list;
	list_count = list->count;
	sst = idx->sst;
	log = idx->log;


	if(list == NULL)
		goto merge_out;

	start = get_ustime_sec();

	pthread_mutex_lock(idx->merge_mutex);
	sst_merge(sst, list, 0);
	idx->park->list = NULL;
	pthread_mutex_unlock(idx->merge_mutex);

	end = get_ustime_sec();
	cost = end - start;
	if (cost > idx->max_merge_time) {
		idx->slowest_merge_count = list_count;
		idx->max_merge_time = cost;
	}

	log_remove(log, lsn);

merge_out:
	if (list) {
		pthread_mutex_lock(idx->listfree_mutex);
		skiplist_free(list);
		pthread_mutex_unlock(idx->listfree_mutex);
	}

	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

struct index *index_new(const char *basedir, int max_mtbl_size, int tolog)
{
	char dbfile[FILE_PATH_SIZE];
	struct index *idx = calloc(1, sizeof(struct index));
	struct idx_park *park = calloc(1, sizeof(struct idx_park));

	if (!idx || !park)
		__PANIC("memory less when index_new, abort...");

	ensure_dir_exists(basedir);
	
	idx->max_mtbl = 1;
	idx->max_mtbl_size = max_mtbl_size;
	memset(idx->basedir, 0, FILE_PATH_SIZE);
	memcpy(idx->basedir, basedir, FILE_PATH_SIZE);

	idx->sst = sst_new(idx->basedir);
	idx->list = skiplist_new(max_mtbl_size);
	idx->merge_mutex = malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(idx->merge_mutex, NULL);


	idx->listfree_mutex = malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(idx->listfree_mutex, NULL);

	/* container */
	park->lsn = idx->lsn;
	idx->park = park;

	/* log */
	idx->log = log_new(idx->basedir, tolog);

	/*
	 * Log Recovery Processes:
	 * 1) read old log file and add entries to memtable
	 * 2) read new log file and add entries to memtable
	 * 3) merge the current active log's memtable
	 * 4) remove old log file,new log file
	 * 5) create new memtable and log file
	 */
	if (log_recovery(idx->log, idx->list)) {
		/* Merge log entries */
		sst_merge(idx->sst, idx->list, 1);

		/* Remove old&new log files */
		remove(idx->log->log_new);
		remove(idx->log->log_old);

		/* Create new memtable */
		idx->list = skiplist_new(idx->max_mtbl_size);
	}

	/* Create new log:0.log */
	log_next(idx->log, 0);

	memset(dbfile, 0, FILE_PATH_SIZE);
	snprintf(dbfile, FILE_PATH_SIZE, "%s/%s", idx->basedir, DB_NAME);
	idx->db_rfd = open(dbfile, LSM_OPEN_FLAGS, 0644);
	if (idx->db_rfd == -1)
		__PANIC("index read fd error");

	/* Detached thread attr */
	pthread_attr_init(&idx->attr);
	pthread_attr_setdetachstate(&idx->attr, PTHREAD_CREATE_DETACHED);

	return idx;
}

int index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	uint64_t value_offset;
	struct skiplist *list, *new_list;

	if (sk->len >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length#%d more than MAX_KEY_SIZE$%d"
				, sk->len
				, NESSDB_MAX_KEY_SIZE);
		return 0;
	}

	value_offset = log_append(idx->log, sk, sv);
	list = idx->list;

	if (!list) {
		__PANIC("List<%d> is NULL", idx->lsn);
		return 0;
	}

	if (!skiplist_notfull(list)) {
		idx->bg_merge_count++;

		/* If the detached-merge thread isnot finished,hold on it 
		 * Notice: it will block the current process, 
		 * but it happens only once in a thousand years on production environment.
		*/
		pthread_mutex_lock(idx->merge_mutex);

		/* Start to merge with detached thread */
		pthread_t tid;
		idx->park->list = list;
		idx->park->lsn = idx->lsn;

		pthread_create(&tid, &idx->attr, _merge_job, idx);

		idx->mtbl_rem_count = 0;
		/* New mtable is born */
		new_list = skiplist_new(idx->max_mtbl_size);
		idx->list = new_list;

		idx->lsn++;
		log_next(idx->log, idx->lsn);
		pthread_mutex_unlock(idx->merge_mutex);
	}
	skiplist_insert(idx->list, sk->data, value_offset, sv == NULL ? DEL : ADD);
	
	/* Add to Bloomfilter */
	if (sv) {
		bloom_add(idx->sst->bloom, sk->data);
	} else
		idx->mtbl_rem_count++;

	return 1;
}

/*
 * When db is normally closed, flush current active memtable to disk sst
 */
void _index_flush(struct index *idx)
{
	int list_count;
	struct skiplist *list;
	long long start, cost;

	/* Waiting  bg merging thread done */

	list = idx->list;
	list_count = list->count;

	if (list && list->count > 0) {
		start = get_ustime_sec();

		pthread_mutex_lock(idx->merge_mutex);
		sst_merge(idx->sst, list, 0);
		pthread_mutex_unlock(idx->merge_mutex);

		if (list) {
			pthread_mutex_lock(idx->listfree_mutex);
			skiplist_free(list);
			pthread_mutex_unlock(idx->listfree_mutex);
		}

		cost = get_ustime_sec() - start;

		if (cost > idx->max_merge_time) {
			idx->slowest_merge_count = list_count;
			idx->max_merge_time = cost;
		}

		log_remove(idx->log, idx->lsn);
	}

	int log_idx_fd = idx->log->log_wfd;
	int log_db_fd = idx->log->db_wfd;

	if (log_idx_fd > 0) {
		if (fsync(log_idx_fd) == -1)
			__ERROR("fsync log error, %d, %s"
					, errno
					, strerror(errno));
	}

	if (log_db_fd > 0) {
		if (fsync(log_db_fd) == -1)
			__ERROR("fsync db error, %s, %s"
					, errno
					, strerror(errno));
	}
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

	if (sk->len >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length#%d more than MAX_KEY_SIZE$%d"
				, sk->len
				, NESSDB_MAX_KEY_SIZE);
		return 0;
	}

	/* 
	 * 0) Get from bloomfilter,if bloom_get return 1,next
	 * 1) First lookup from active memtable
 	 * 2) Then from merge memtable 
 	 * 3) Last from sst on-disk indexes
 	 */
	ret = bloom_get(idx->sst->bloom, sk->data);
	if (ret == 0)
		return 0;
	
	idx->bloom_hits++;
	cur_list = idx->list;
	node = skiplist_lookup(cur_list, sk->data);
	if (node){
		if(node->opt == DEL) {
			ret  = -1;
			goto out_get;
		}
		value_off = node->val;
	} else {
		pthread_mutex_lock(idx->listfree_mutex);
		merge_list = idx->park->list;
		if (merge_list) {
			node = skiplist_lookup(merge_list, sk->data);
			if (node && node->opt == ADD )
				value_off = node->val;
		}
		pthread_mutex_unlock(idx->listfree_mutex);
	}

	if (value_off == 0UL)
		value_off = sst_getoff(idx->sst, sk);

	if (value_off != 0UL) {
		if (n_lseek(idx->db_rfd, value_off, SEEK_SET) == -1) {
			__ERROR("seek value offset error, %s, %s"
					, errno
					, strerror(errno));
			goto out_get;
		}

		result = read(idx->db_rfd, &value_len, sizeof(int));
		if(FILE_ERR(result)) {
			ret = -1;
			goto out_get;
		}

		if(result == sizeof(int)) {
			char *data = calloc(1, value_len + 1);

			if (!data)
				__ERROR("mallc data  error, %s, %s"
						, errno
						, strerror(errno));

			result = read(idx->db_rfd, data, value_len);
			data[value_len] = 0;

			if(FILE_ERR(result)) {
				free(data);
				ret = -1;
				goto out_get;
			}
			sv->len = value_len;
			sv->data = data;
			return 1;
		}
	} else
		return 0;

out_get:
	return ret;
}

uint64_t index_allcount(struct index *idx)
{
	int i, size;
	uint64_t c = 0UL;
	
	size = idx->sst->meta->size;
	for (i = 0; i < size; i++)
		c += idx->sst->meta->nodes[i].count;

	return c;
}
void index_free(struct index *idx)
{
	_index_flush(idx);

	if (idx->max_merge_time > 0) {
		__DEBUG("max merge time:%lu sec;"
				"the slowest merge-count:%d and merge-speed:%.1f/sec"
				, idx->max_merge_time
				, idx->slowest_merge_count
				, (double) (idx->slowest_merge_count / idx->max_merge_time));
	}

	if (idx->merge_mutex)
		free(idx->merge_mutex);
	if (idx->listfree_mutex)
		free(idx->listfree_mutex);

	log_free(idx->log);

	if (idx->db_rfd > 0)
		close(idx->db_rfd);

	sst_free(idx->sst);
	free(idx->park);
	free(idx);
}
