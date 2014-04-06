/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "cache.h"

/**
 * @file cache.c
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
void _free_cpair(struct cache *c, struct cpair *cp)
{
	mutex_lock(&c->mtx);
	c->cp_count--;
	c->cache_size -= cp->v->attr.newsz;
	mutex_unlock(&c->mtx);

	node_free(cp->v);
	xfree(cp);
}

static inline int _must_evict(struct cache *c)
{
	int must;

	mutex_lock(&c->mtx);
	must = ((c->cache_size > c->opts->cache_limits_bytes));
	mutex_unlock(&c->mtx);

	return must;
}

static inline int _need_evict(struct cache *c)
{
	int need;

	mutex_lock(&c->mtx);
	need = (c->cache_size > (c->opts->cache_limits_bytes *
	                         c->opts->cache_high_watermark / 100));
	mutex_unlock(&c->mtx);

	return need;
}

/* try to evict(free) a pair */
void _try_evict_pair(struct cache *c, struct cpair *p)
{
	int is_dirty;
	struct tree_callback *tcb = p->cf->tcb;
	struct tree *tree = (struct tree*)p->cf->args;

	/* others can't get&pin this node */
	cpair_locked_by_key(c->table, p->k);

	/* write dirty to disk */
	write_lock(&p->value_lock);
	is_dirty = node_is_dirty(p->v);
	if (is_dirty) {
		tcb->flush_node(tree, p->v);
		node_set_nondirty(p->v);
	}
	write_unlock(&p->value_lock);

	/* remove from list */
	write_lock(&c->clock_lock);
	cpair_list_remove(c->clock, p);
	cpair_htable_remove(c->table, p);
	write_unlock(&c->clock_lock);
	cpair_unlocked_by_key(c->table, p->k);

	/* here, none can grant the p */
	_free_cpair(c, p);
}

void _cache_dump(struct cache *c, const char *msg)
{
	struct cpair *cur;
	uint64_t all_size = 0UL;

	cur = c->clock->head;
	printf("----%s: cache dump:\n", msg);
	while (cur) {
		printf("\t nid[%" PRIu64 "],\tnodesz[%d]MB,\tdirty[%d],"
		       "\tnum_readers[%d],\tnum_writers[%d]\n",
		       cur->v->nid,
		       (int)(cur->v->attr.newsz / (1024 * 1024)),
		       node_is_dirty(cur->v),
		       cur->value_lock.num_readers,
		       cur->value_lock.num_writers
		      );
		all_size += (cur->v->attr.newsz);
		cur = cur->list_next;
	}
	printf("---all size\t[%" PRIu64 "]MB\n", all_size / (1024 * 1024));
	printf("---limit size\t[%" PRIu64 "]MB\n\n",
	       c->opts->cache_limits_bytes / (1024 * 1024));
}

/* pick one pair from clock list */
void _run_eviction(struct cache *c)
{
	struct cpair *cur;
	struct cpair *nxt;

	read_lock(&c->clock_lock);
	cur = c->clock->head;
	read_unlock(&c->clock_lock);
	if (!cur)
		return;

	while (_need_evict(c) && cur) {
		int isroot = 0;
		int num_readers = 0;
		int num_writers = 0;

		read_lock(&c->clock_lock);
		isroot = cur->v->isroot;
		num_readers = cur->value_lock.num_readers;
		num_writers = cur->value_lock.num_writers;
		nxt = cur->list_next;
		read_unlock(&c->clock_lock);

		if ((num_readers == 0)
		    && (num_writers == 0)
		    && (isroot == 0)) {
			_try_evict_pair(c, cur);
			cond_signalall(&c->condvar);
		}
		cur = nxt;
	}
}

static void *flusher_cb(void *arg)
{
	struct cache *c;
	c = (struct cache*)arg;

	_run_eviction(c);

	return NULL;
}

struct cache *cache_new(struct options *opts) {
	struct cache *c;

	c = xcalloc(1, sizeof(*c));
	rwlock_init(&c->clock_lock);
	mutex_init(&c->mtx);
	cond_init(&c->condvar);

	c->clock = cpair_list_new();
	c->table = cpair_htable_new();

	c->opts = opts;
	gettime(&c->last_checkpoint);
	c->flusher = cron_new(flusher_cb, opts->cache_flush_period_ms);
	if (cron_start(c->flusher, (void*)c) != 1) {
		__PANIC("%s",
		        "flushe cron start error, will exit");
		goto ERR;
	}

	return c;
ERR:
	xfree(c);
	return NULL;
}

/*
 * REQUIRES:
 * a) cache list write-lock
 */
void _cache_insert(struct cache *c, struct cpair *p)
{
	cpair_list_add(c->clock, p);
	cpair_htable_add(c->table, p);
	c->cp_count++;
}

/* make room for writes */
void _make_room(struct cache *c)
{
	/* check cache limits, if reaches we should slow down */
	int allow_delay = 1;

	while (1) {
		int must = _must_evict(c);
		int need = _need_evict(c);

		if (must) {
			__WARN("must evict, waiting..., cache limits [%" PRIu64 "]MB",
			       c->cache_size / 1048576);
			cond_wait(&c->condvar);
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
int cache_get_and_pin(struct cache_file *cf,
                      NID k,
                      struct node **n,
                      enum lock_type locktype)
{
	struct cpair *p;
	struct cache *c = cf->cache;

TRY_PIN:
	/* make room for me, please */
	_make_room(c);

	cpair_locked_by_key(c->table, k);
	p = cpair_htable_find(c->table, k);
	cpair_unlocked_by_key(c->table, k);
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

	cpair_locked_by_key(c->table, k);
	p = cpair_htable_find(c->table, k);
	if (p) {
		/*
		 * if we go here, means that someone got before us
		 * try pin again
		 */
		cpair_unlocked_by_key(c->table, k);
		goto TRY_PIN;
	}

	struct tree_callback *tcb = cf->tcb;
	struct tree *tree = (struct tree*)cf->args;
	int r = tcb->fetch_node(tree, k, n);
	if (r != NESS_OK) {
		__PANIC("fetch node from disk error, nid [%" PRIu64 "], errno %d",
		        k,
		        r);
		goto ERR;
	}

	p = cpair_new();
	cpair_init(p, *n, cf);

	/* add to cache list */
	write_lock(&c->clock_lock);
	_cache_insert(c, p);
	write_unlock(&c->clock_lock);

	if (locktype != L_READ) {
		write_lock(&p->value_lock);
	} else {
		read_lock(&p->value_lock);
	}

	cpair_unlocked_by_key(c->table, k);

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int cache_create_node_and_pin(struct cache_file *cf,
                              uint32_t height,
                              uint32_t children,
                              struct node **n)
{
	NID next_nid;
	struct cpair *p;
	struct node *new_node;
	struct cache *c = cf->cache;

	next_nid = hdr_next_nid((struct tree*)cf->args);
	if (height == 0)
		new_node = leaf_alloc_empty(next_nid);
	else
		new_node = nonleaf_alloc_empty(next_nid, height, children);

	p = cpair_new();
	cpair_init(p, new_node, cf);

	/* default lock type is L_WRITE */
	write_lock(&p->value_lock);

	*n = new_node;

	/* add to cache list */
	write_lock(&c->clock_lock);
	_cache_insert(c, p);
	write_unlock(&c->clock_lock);

	return NESS_OK;
}

void cache_unpin_readonly(struct cache_file *cf, struct node *n)
{
	struct cpair *p;
	struct cache *c = cf->cache;

	/*
	 * here, we don't need a hashtable array lock,
	 * since we have hold the pair->value_lock,
	 * others(evict thread) can't remove it from cache
	 */
	p = cpair_htable_find(c->table, n->nid);
	nassert(p);

	write_unlock(&p->value_lock);
}

void cache_unpin(struct cache_file *cf, struct node *n)
{
	struct cpair *p;
	struct cache *c = cf->cache;

	/*
	 * here, we don't need a hashtable array lock,
	 * since we have hold the pair->value_lock,
	 * others(evict thread) can't remove it from cache
	 */
	p = cpair_htable_find(c->table, n->nid);
	nassert(p);

	n->attr.oldsz = n->attr.newsz;
	n->attr.newsz = node_size(n);
	mutex_lock(&c->mtx);
	c->cache_size += (n->attr.newsz - n->attr.oldsz);
	mutex_unlock(&c->mtx);

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
void cache_cpair_value_swap(struct cache_file *cf, struct node *a, struct node *b)
{
	struct cpair *cpa;
	struct cpair *cpb;
	struct cache *c = cf->cache;

	cpa = cpair_htable_find(c->table, a->nid);
	nassert(cpa);

	cpb = cpair_htable_find(c->table, b->nid);
	nassert(cpb);

	cpa->v = b;
	cpb->v = a;
}


struct cache_file *cache_file_create(struct cache *c,
                                     struct tree_callback *tcb,
                                     void *args) {
	struct cache_file *cf;

	cf = xcalloc(1, sizeof(*cf));
	cf->cache = c;
	cf->tcb = tcb;
	cf->args = args;
	cf->filenum = atomic32_increment(&c->filenum);

	mutex_lock(&c->mtx);
	if (!c->cf_first) {
		c->cf_first = c->cf_last = cf;
	} else {
		cf->prev = c->cf_last;
		c->cf_last->next = cf;
		c->cf_last = cf;
	}
	mutex_unlock(&c->mtx);

	return cf;
}

int cache_file_remove(struct cache *c, int filenum)
{
	(void)c;
	(void)filenum;

	return (-1);
}


struct tree *cache_get_tree_by_filenum(struct cache *c, FILENUM fn) {
	struct cache_file *cf = c->cf_first;

	while (cf) {
		if (cf->filenum == (int)fn.fileid)
			return (struct tree*)cf->args;
		cf = cf->next;
	}

	return NULL;
}

void cache_close(struct cache *c)
{
	int r;
	struct cpair *cur;
	struct cpair *nxt;

	/* a) stop the flusher cron */
	cron_free(c->flusher);

	/* b) write back nodes to disk */
	cur = c->clock->head;
	while (cur) {
		nxt = cur->list_next;
		if (node_is_dirty(cur->v)) {
			struct tree *tree;
			struct tree_callback *tcb;

			write_lock(&cur->disk_mtx);
			tree = (struct tree*)cur->cf->args;
			tcb = cur->cf->tcb;
			r = tcb->flush_node(tree, cur->v);
			write_unlock(&cur->disk_mtx);
			if (r != NESS_OK) {
				__PANIC("serialize node to disk error, fd [%d]",
				        tree->fd);
			}
			node_set_nondirty(cur->v);
		}
		_free_cpair(c, cur);
		cur = nxt;
	}

	struct cache_file *cf_cur;
	struct cache_file *cf_nxt;

	/* TODO: (BohuTANG) we need a lock */
	cf_cur = c->cf_first;
	while (cf_cur) {
		struct tree *tree;
		struct tree_callback *tcb;

		tree = (struct tree*)cf_cur->args;
		tcb = cf_cur->tcb;
		cf_nxt = cf_cur->next;

		r = tcb->flush_hdr(tree);
		if (r != NESS_OK) {
			__PANIC("serialize hdr[%d] to disk error",
			        tree->fd);
		}
		xfree(cf_cur);
		cf_cur = cf_nxt;
	}

}

void cache_free(struct cache *c)
{
	cache_close(c);
	cpair_htable_free(c->table);
	cpair_list_free(c->clock);
	xfree(c);
}
