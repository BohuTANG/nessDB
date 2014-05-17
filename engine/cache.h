/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CACHE_H_
#define nessDB_CACHE_H_

#include "internal.h"
#include "cpair.h"
#include "tree.h"

/**
 * @file cache.h
 * @brief virtual cache(VCACHE)
 * if you want to implement a sub cache of vcache
 * you MUST implement the API defined in cache_operations
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

struct cache *cache_new(struct options *opts);
void cache_free(struct cache *c);

int cache_get_and_pin(struct cache_file *cf,
                      NID k,
                      struct node **n,
                      enum lock_type locktype);
int cache_create_node_and_pin(struct cache_file *cf,
                              uint32_t height,
                              uint32_t children,
                              struct node **n);

void cache_unpin(struct cache_file *cf, struct node *n);

void cache_cpair_value_swap(struct cache_file *cf,
                            struct node *a,
                            struct node *b);

struct cache_file *cache_file_create(struct cache *c,
                                     struct tree_callback *tcb,
                                     void *args);
int cache_file_remove(struct cache *c, int filenum);

struct tree *cache_get_tree_by_filenum(struct cache *c, FILENUM fn);

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

	ness_rwlock_t clock_lock;	/* barriers for list */
	ness_mutex_t mtx;		/* barriers for cache variables */
	ness_cond_t condvar;
};

#endif /* nessDB_CACHE_H_ */
