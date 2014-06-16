/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CACHE_H_
#define nessDB_CACHE_H_

#include "internal.h"
#include "kibbutz.h"
#include "tree.h"
#include "posix.h"

/**
 * @file cache.h
 * @brief virtual cache(VCACHE)
 */

struct cache_file {
	int fd;
	int filenum;
	void *args;
	struct cache *cache;
	struct tree_callback *tcb;
	struct cache_file *prev;
	struct cache_file *next;
};

struct cpair {
	NID k;
	struct node *v;
	struct cache_file *cf;
	struct cpair *list_next;
	struct cpair *list_prev;
	struct cpair *hash_next;

	ness_mutex_t mtx;		/* barriers for pair */
	ness_rwlock_t disk_lock;		/* barriers for reading/writing to disk */
	ness_rwlock_t value_lock;		/* barriers for value */
};

struct cache {
	int filenum;
	uint32_t cp_count;
	uint64_t cache_size;
	struct cpair_list *clock;	/* list of cache, in clock order, the newst is in head */
	struct cpair_htable *table;	/* hashtable, for finding */
	struct cron *flusher;		/* flush/evict nodes to disk when cache limits reached */
	struct options *opts;
	struct cache_file *cf_first;
	struct cache_file *cf_last;
	struct timespec last_checkpoint;

	ness_mutex_t mtx;			/* barriers for cache variables */
	ness_rwlock_t clock_lock;		/* barriers for list */
	ness_cond_t wait_makeroom;
	ness_mutex_t makeroom_mtx;		/* barriers for cache variables */
	struct kibbutz *c_kibbutz;
};

struct cache *cache_new(struct options *opts);
void cache_free(struct cache *c);

int cache_create_node_and_pin(struct cache_file *cf, uint32_t height, uint32_t children, struct node **n);
int cache_get_and_pin(struct cache_file *cf, NID k, struct node **n, enum lock_type locktype);
void cache_unpin(struct cache_file *cf, struct node *n);

struct tree *cache_get_tree_by_filenum(struct cache *c, FILENUM fn);
struct cache_file *cache_file_create(struct cache *c, struct tree_callback *tcb, void *args);
int cache_file_remove(struct cache *c, int filenum);

#endif /* nessDB_CACHE_H_ */
