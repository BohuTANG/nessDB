/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "dbcache.h"
#include "tree.h"

/**
 * @file dbcache.c
 * @brief this file is a version implementation of vcache
 */

#define PAIR_LIST_SIZE (1<<20)

/* hashtable array lock */
static inline void cpair_locked_by_key(struct cpair_htable *table, NID key)
{
	mutex_lock(&table->mutexes[key & (PAIR_LIST_SIZE - 1)].aligned_mtx);
}

/* hashtable array unlock */
static inline void cpair_unlocked_by_key(struct cpair_htable *table, NID key)
{
	mutex_unlock(&table->mutexes[key & (PAIR_LIST_SIZE - 1)].aligned_mtx);
}

/*
 * free a cpair&node
 *
 * REQUIRES:
 * a) hashtable array lock on this cpair solt
 */
void _free_cpair(struct dbcache *dbc, struct cpair *cp)
{
	mutex_lock(&dbc->mtx);
	dbc->cp_count--;
	dbc->cache_size -= cp->v->attr.newsz;
	mutex_unlock(&dbc->mtx);

	node_free(cp->v);
	xfree(cp);
}

static inline int _must_evict(struct dbcache *dbc)
{
	int must;
	
	mutex_lock(&dbc->mtx);
	must = ((dbc->cache_size > dbc->opts->cache_limits_bytes));
	mutex_unlock(&dbc->mtx);

	return must;
}

static inline int _need_evict(struct dbcache *dbc)
{
	int need;

	mutex_lock(&dbc->mtx);
	need = (dbc->cache_size > (dbc->opts->cache_limits_bytes *
				dbc->opts->cache_high_watermark / 100));
	mutex_unlock(&dbc->mtx);

	return need;
}

/* try to evict(free) a pair */
void _try_evict_pair(struct dbcache *dbc, struct cpair *p)
{
	int is_dirty;
	struct tree *tree = dbc->cfile.tree;
	struct tree_operations *t_op = tree->t_op;

	/* others can't get&pin this node */
	cpair_locked_by_key(dbc->table, p->k);

	/* write dirty to disk */
	write_lock(&p->value_lock);
	is_dirty = node_is_dirty(p->v);
	if (is_dirty) {
		t_op->flush_node(tree, p->v);
		node_set_nondirty(p->v);
	}
	write_unlock(&p->value_lock);

	/* remove from list */
	write_lock(&dbc->clock_lock);
	cpair_list_remove(dbc->clock, p);
	cpair_htable_remove(dbc->table, p);
	write_unlock(&dbc->clock_lock);
	cpair_unlocked_by_key(dbc->table, p->k);

	/* here, none can grant the p */
	_free_cpair(dbc, p);
}

void _cache_dump(struct dbcache *dbc, const char *msg)
{
	struct cpair *cur;
	uint64_t all_size = 0UL;

	cur = dbc->clock->head;
	printf("----%s: cache dump:\n", msg);
	while(cur) {
		printf("\t nid[%" PRIu64 "],\tnodesz[%d]MB,\tdirty[%d],"
				"\tnum_readers[%d],\tnum_writers[%d]\n",
				cur->v->nid,
				(int)(cur->v->attr.newsz/ (1024 * 1024)),
				node_is_dirty(cur->v),
				cur->value_lock.num_readers,
				cur->value_lock.num_writers
		      );
		all_size += (cur->v->attr.newsz);
		cur = cur->list_next;
	}
	printf("---all size\t[%" PRIu64 "]MB\n", all_size / (1024 * 1024));
	printf("---limit size\t[%" PRIu64 "]MB\n\n",
			dbc->opts->cache_limits_bytes / (1024 * 1024));
}

/* pick one pair from clock list */
void _run_eviction(struct dbcache *dbc)
{
	struct cpair *cur;
	struct cpair *nxt;

	read_lock(&dbc->clock_lock);
	cur = dbc->clock->head;
	read_unlock(&dbc->clock_lock);
	if (!cur)
		return;

	while (_need_evict(dbc) && cur) {
		int isroot = 0;
		int num_readers = 0;
		int num_writers = 0;

		read_lock(&dbc->clock_lock);
		isroot = cur->v->isroot;
		num_readers = cur->value_lock.num_readers;
		num_writers = cur->value_lock.num_writers;
		nxt = cur->list_next;
		read_unlock(&dbc->clock_lock);

		if ((num_readers == 0)
				&&(num_writers == 0)
				&& (isroot == 0)) {
			_try_evict_pair(dbc, cur);
			cond_signalall(&dbc->condvar);
		}
		cur = nxt;
	}
}

static void *flusher_cb(void *arg)
{
	struct dbcache *dbc;
	dbc = (struct dbcache*)arg;

	_run_eviction(dbc);

	return NULL;
}

struct cache *dbcache_new(struct options *opts)
{
	struct dbcache *dbc;
	
	dbc = xcalloc(1, sizeof(*dbc));
	rwlock_init(&dbc->clock_lock);
	mutex_init(&dbc->mtx);
	cond_init(&dbc->condvar);

	dbc->clock = cpair_list_new();
	dbc->table = cpair_htable_new();

	dbc->opts = opts;
	gettime(&dbc->last_checkpoint);
	dbc->flusher = cron_new(flusher_cb, opts->cache_flush_period_ms);
	if (cron_start(dbc->flusher, (void*)dbc) != 1) {
		__PANIC("%s",
			"flushe cron start error, will exit");
		goto ERR;
	}

	/* vritual cache */
	static struct cache_operations dbcache_operations = {
		.cache_get_and_pin = &dbcache_get_and_pin,
		.cache_create_node_and_pin = dbcache_create_node_and_pin,
		.cache_cpair_value_swap = dbcache_cpair_value_swap,
		.cache_unpin = dbcache_unpin,
		.cache_unpin_readonly = dbcache_unpin_readonly,
		.cache_file_register = dbcache_file_register,
		.cache_file_unregister = dbcache_file_unregister,
	};
	struct cache *c = xcalloc(1, sizeof(*c));
	c->c_op = &dbcache_operations;
	c->extra = dbc;

	return c;
ERR:
	xfree(c);
	return NULL;
}

/*
 * REQUIRES:
 * a) cache list write-lock
 */
void _cache_insert(struct dbcache *dbc, struct cpair *p)
{
	cpair_list_add(dbc->clock, p);
	cpair_htable_add(dbc->table, p);
	dbc->cp_count++;
}

/* make room for writes */
void _make_room(struct dbcache *dbc)
{
	/* check cache limits, if reaches we should slow down */
	int allow_delay = 1;

	while (1) {
		int must = _must_evict(dbc);
		int need = _need_evict(dbc);

		if (must) {
			__WARN("must evict, waiting..., cache limits [%" PRIu64 "]MB",
				dbc->cache_size / 1048576);
			cond_wait(&dbc->condvar);
		} else if (allow_delay && need) {
			/*
			 * slow down
			 */
			usleep(100);
			allow_delay = 0;
		} else {
			break;
		}
	}
}

/**********************************************************************
 *
 *  vcache API begin
 *
 **********************************************************************/

/*
 * REQUIRES:
 * a) cache list read(write) lock
 *
 * PROCESSES:
 * a) lock hash chain
 * b) find pair via key
 * c) if found, lock the value and return
 * d) if not found:
 *	d0) list lock
 *	d1) add to cache/evict/clean list
 *	d2) value lock
 *	d3) list unlock
 *	d4) disk-mtx lock
 *	d5) fetch from disk
 *	d6) disk-mtx unlock
 */
int dbcache_get_and_pin(struct cache *c,
		NID k,
		struct node **n,
		enum lock_type locktype)
{
	struct cpair *p;
	struct dbcache *dbc = (struct dbcache*)c->extra;

TRY_PIN:
	/* make room for me, please */
	_make_room(dbc);

	cpair_locked_by_key(dbc->table, k);
	p = cpair_htable_find(dbc->table, k);
	cpair_unlocked_by_key(dbc->table, k);
	if (p) {
		if (locktype != L_READ) {
			if (!try_write_lock(&p->value_lock))
				goto TRY_PIN;
		} else {
			if (!try_read_lock(&p->value_lock))
				goto TRY_PIN;
		}

		*n = p->v;
		return NESS_OK;
	}

	cpair_locked_by_key(dbc->table, k);
	p = cpair_htable_find(dbc->table, k);
	if (p) {
		/*
		 * if we go here, means that someone got before us
		 * try pin again
		 */
		cpair_unlocked_by_key(dbc->table, k);
		goto TRY_PIN;
	}

	struct tree *tree = dbc->cfile.tree;
	struct tree_operations *t_op = tree->t_op;
	int r = t_op->fetch_node(tree, k, n);
	if (r != NESS_OK) {
		__PANIC("fetch node from disk error, nid [%" PRIu64 "], errno %d",
				k,
				r);
		goto ERR;
	}

	p = cpair_new();
	cpair_init(p, *n);

	/* add to cache list */
	write_lock(&dbc->clock_lock);
	_cache_insert(dbc, p);
	write_unlock(&dbc->clock_lock);

	if (locktype != L_READ) {
		write_lock(&p->value_lock);
	} else {
		read_lock(&p->value_lock);
	}

	cpair_unlocked_by_key(dbc->table, k);

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int dbcache_create_node_and_pin(struct cache *c,
		uint32_t height,
		uint32_t children,
		struct node **n)
{
	NID next_nid;
	struct cpair *p;
	struct node *new_node;
	struct dbcache *dbc = (struct dbcache*)c->extra;
	struct tree *tree = dbc->cfile.tree;

	next_nid = hdr_next_nid(tree);

	if (height == 0)
		new_node = leaf_alloc_empty(next_nid);
	else
		new_node = nonleaf_alloc_empty(next_nid, height, children);

	p = cpair_new();
	cpair_init(p, new_node);

	/* default lock type is L_WRITE */
	write_lock(&p->value_lock);

	*n = new_node;

	/* add to cache list */
	write_lock(&dbc->clock_lock);
	_cache_insert(dbc, p);
	write_unlock(&dbc->clock_lock);

	return NESS_OK;
}

void dbcache_unpin_readonly(struct cache *c, struct node *n)
{
	struct cpair *p;
	struct dbcache *dbc = (struct dbcache*)c->extra;

	/*
	 * here, we don't need a hashtable array lock,
	 * since we have hold the pair->value_lock,
	 * others(evict thread) can't remove it from cache
	 */
	p = cpair_htable_find(dbc->table, n->nid);
	nassert(p);

	write_unlock(&p->value_lock);
}

void dbcache_unpin(struct cache *c, struct node *n)
{
	struct cpair *p;
	struct dbcache *dbc = (struct dbcache*)c->extra;

	/*
	 * here, we don't need a hashtable array lock,
	 * since we have hold the pair->value_lock,
	 * others(evict thread) can't remove it from cache
	 */
	p = cpair_htable_find(dbc->table, n->nid);
	nassert(p);

	n->attr.oldsz = n->attr.newsz;
	n->attr.newsz = node_size(n);
	mutex_lock(&dbc->mtx);
	dbc->cache_size += (n->attr.newsz - n->attr.oldsz);
	mutex_unlock(&dbc->mtx);

	write_unlock(&p->value_lock);
}

/*
 * swap the cache pair value and key
 *
 * REQUIRES:
 * a) node a lock(L_WRITE)
 * b) node b locked(L_WRITE)
 *
 */
void dbcache_cpair_value_swap(struct cache *c, struct node *a, struct node *b)
{
	struct cpair *cpa;
	struct cpair *cpb;
	struct dbcache *dbc = (struct dbcache*)c->extra;

	cpa = cpair_htable_find(dbc->table, a->nid);
	nassert(cpa);

	cpb = cpair_htable_find(dbc->table, b->nid);
	nassert(cpb);

	cpa->v = b;
	cpb->v = a;
}

void dbcache_file_register(struct cache *c, struct cache_file *cfile)
{
	nassert(cfile->fd > -1);
	nassert(cfile->tree);

	struct dbcache *dbc = (struct dbcache*)c->extra;
	dbc->cfile = *cfile;
}

void dbcache_file_unregister(struct cache *c, struct cache_file *cfile)
{
	/* TODO: not use */
	(void)c;
	(void)cfile;
	return;
}

/**********************************************************************
 *
 *  vcache API end
 *
 **********************************************************************/

void dbcache_close(struct dbcache *dbc)
{
	int r;
	struct cpair *cur;
	struct cpair *nxt;
	struct tree *tree = dbc->cfile.tree;
	struct tree_operations *t_op = tree->t_op;

	/* a) stop the flusher cron */
	cron_free(dbc->flusher);

	/* b) write back nodes to disk */
	cur = dbc->clock->head;
	while (cur) {
		nxt = cur->list_next;
		if (node_is_dirty(cur->v)) {
			write_lock(&cur->disk_mtx);
			r = t_op->flush_node(tree, cur->v);
			write_unlock(&cur->disk_mtx);
			if (r != NESS_OK) {
				__PANIC("serialize node[%d] to disk error",
						dbc->cfile.fd);
			}
			node_set_nondirty(cur->v);
		}
		_free_cpair(dbc, cur);
		cur = nxt;
	}

	r = t_op->flush_hdr(tree);
	if (r != NESS_OK) {
		__PANIC("serialize hdr[%d] to disk error",
				dbc->cfile.fd);
	}
}

int compaction_begin(struct dbcache *dbc)
{
	(void)dbc;

	return NESS_OK;
}

/*
 * PROCESSES:
 * a) wirte-back all dirty nodes to disk
 * b) write-back tree header to disk
 * c) to run-eviction if need_evict true
 */
int compaction_finish(struct dbcache *dbc)
{
	(void)dbc;
	return NESS_OK;
}

void dbcache_free(struct cache *c)
{
	struct dbcache *dbc = (struct dbcache*)c->extra;

	dbcache_close(dbc);
	cpair_htable_free(dbc->table);
	cpair_list_free(dbc->clock);
	xfree(dbc);
	xfree(c);
}
