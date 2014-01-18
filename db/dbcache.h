/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_DBCACHE_H_
#define nessDB_DBCACHE_H_

#include "internal.h"
#include "options.h"
#include "cache.h"

struct dbcache {
	int fd;
	uint32_t cp_count;
	uint64_t cache_size;

	struct cpair_list *clock;	/* list of cache, in clock order, the newst is in head */
	struct cpair_htable *table;	/* hashtable, for finding */

	struct cache_file cfile;

	struct cron *flusher;		/* flush/evict nodes to disk when cache limits reached */
	struct options *opts;
	struct timespec last_checkpoint;

	ness_rwlock_t clock_lock;	/* barriers for list */
	ness_mutex_t mtx;		/* barriers for cache variables */
	ness_cond_t condvar;
};

/* vcache callback for tree, cache must implement them */
int (dbcache_get_and_pin) (struct cache *c, NID k, struct node **n, enum lock_type locktype);
int (dbcache_create_node_and_pin) (struct cache *c, uint32_t height, uint32_t children, struct node **n);
void (dbcache_cpair_value_swap) (struct cache *c, struct node *a, struct node *b);
void (dbcache_unpin) (struct cache *c, struct node *n);
void (dbcache_unpin_readonly) (struct cache *c, struct node *n);
void (dbcache_file_register) (struct cache *c, struct cache_file *cfile);
void (dbcache_file_unregister) (struct cache *c, struct cache_file *cfile);

struct cache *dbcache_new(struct options *opts);
int compaction_begin(struct dbcache *);
int compaction_finish(struct dbcache *);
void dbcache_free(struct cache *c);

#endif /* nessDB_DBCACHE_H_ */
