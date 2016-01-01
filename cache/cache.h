/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_CACHE_H_
#define nessDB_CACHE_H_

/**
 * @file cache.h
 * @brief virtual cache(VCACHE)
 */

enum lock_type;
struct cache_file {
	int fd;
	int filenum;
	void *hdr;
	struct cache *cache;
	struct cache_file *prev;
	struct cache_file *next;
	struct cpair_list *clock;		/* list of cache, in clock order, the newst is in head */
	struct xtable *xtable;		/* hashtable, for finding */
	struct tree_operations *tree_opts;

	ness_mutex_t mtx;			/* barriers for cache variables */
	ness_rwlock_t clock_lock;		/* barriers for list */
};

struct cpair_attr {
	int nodesz;
};

struct cpair {
	NID k;
	void *v;
	int pintype;
	struct cpair_attr attr;
	struct cache_file *cf;
	struct cpair *list_next;
	struct cpair *list_prev;
	struct cpair *hash_next;

	ness_mutex_t mtx;			/* barriers for pair */
	ness_rwlock_t disk_lock;		/* barriers for reading/writing to disk */
	ness_rwlock_t value_lock;		/* barriers for value */
};

struct cache {
	int filenum;
	uint32_t cp_count;
	struct ness_cron *flusher;			/* flush/evict nodes to disk when cache limits reached */
	struct cache_file *cf_first;
	struct cache_file *cf_last;
	struct timespec last_checkpoint;

	struct env *e;
	struct quota *quota;
	struct kibbutz *c_kibbutz;

	ness_mutex_t mtx;			/* barriers for cache variables */
};

struct cache *cache_new(struct env *e);
void cache_free(struct cache *c);

int cache_put_and_pin(struct cache_file *cf, NID k, void *v);
int cache_get_and_pin(struct cache_file *cf, NID k, void **n, enum lock_type locktype);
void cache_unpin(struct cache_file *cf, struct cpair *cpair);

struct cache_file *cache_file_create(struct cache *c, int fd, void *hdr, struct tree_operations *tree_opts);
void cache_file_free(struct cache_file *cf);

void cache_file_flush_dirty_nodes(struct cache_file *cf);
void cache_file_flush_hdr(struct cache_file *cf);
void cache_file_free(struct cache_file *cf);

#endif /* nessDB_CACHE_H_ */
