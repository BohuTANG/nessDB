/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CACHE_H_
#define nessDB_CACHE_H_

/**
 * @file cache.h
 * @brief virtual cache(VCACHE)
 */

struct tree_callback {
	int (*fetch_node_cb)(int fd, void *hdr, NID nid, void **n);
	int (*flush_node_cb)(int fd, void *hdr, void *n);
	int (*free_node_cb)(void *n);
	int (*fetch_hdr_cb)(int fd, void *hdr);
	int (*flush_hdr_cb)(int fd, void *hdr);
	int (*cache_put_cb)(void *n, void *cpair);
	int (*node_is_dirty_cb)(void *n);
	int (*node_set_nondirty_cb)(void *n);
};

struct cache_file {
	int fd;
	int filenum;
	void *hdr;
	struct cache *cache;
	struct tree_callback *tcb;
	struct cache_file *prev;
	struct cache_file *next;
	struct cpair_list *clock;		/* list of cache, in clock order, the newst is in head */
	struct cpair_htable *table;		/* hashtable, for finding */

	ness_mutex_t mtx;			/* barriers for cache variables */
	ness_rwlock_t clock_lock;		/* barriers for list */
};

struct cpair_attr {
	uint32_t nodesz;
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
	uint64_t cache_size;
	struct cron *flusher;			/* flush/evict nodes to disk when cache limits reached */
	struct cache_file *cf_first;
	struct cache_file *cf_last;
	struct timespec last_checkpoint;

	ness_mutex_t mtx;			/* barriers for cache variables */
	ness_cond_t wait_makeroom;
	ness_mutex_t makeroom_mtx;		/* barriers for cache variables */
	struct kibbutz *c_kibbutz;
	struct env *e;
};

struct cache *cache_new(struct env *e);
void cache_free(struct cache *c);

int cache_put_and_pin(struct cache_file *cf, NID k, void *v);
int cache_get_and_pin(struct cache_file *cf, NID k, void **n, enum lock_type locktype);
void cache_unpin(struct cache_file *cf, struct cpair *cpair, struct cpair_attr attr);

struct cache_file *cache_file_create(struct cache *c, int fd, void *hdr, struct tree_callback *tcb);
void cache_file_free(struct cache_file *cf);

void cache_file_flush_dirty_nodes(struct cache_file *cf);
void cache_file_flush_hdr(struct cache_file *cf);
void cache_file_free(struct cache_file *cf);

#endif /* nessDB_CACHE_H_ */
