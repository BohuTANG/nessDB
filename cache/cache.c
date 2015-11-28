/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"
#include "env.h"
#include "c.h"

#define PAIR_LIST_SIZE (5<<10)

int cache_xtable_hash_func(void *k)
{
	struct cpair *c = (struct cpair*)k;

	return (int)c->k;
}

int cache_xtable_compare_func(void *a, void *b)
{
	struct cpair *ca = (struct cpair*)a;
	struct cpair *cb = (struct cpair*)b;

	return (ca->k - cb->k);
}


struct cpair_list {
	struct cpair *head;
	struct cpair *last;
};

struct cpair *cpair_new()
{
	struct cpair *cp;

	cp = xcalloc(1, sizeof(*cp));
	cp->pintype = L_NONE;
	cp->attr.nodesz = 0;
	mutex_init(&cp->mtx);
	ness_rwlock_init(&cp->disk_lock);
	ness_rwlock_init(&cp->value_lock);

	return cp;
}

void cpair_init(struct cpair *cp, NID k, void *value, struct cache_file *cf)
{
	cp->k = k;
	cp->cf = cf;
	cp->v = value;
}

/*******************************
 * list (in clock order)
 ******************************/
struct cpair_list *cpair_list_new()
{
	struct cpair_list *list;

	list = xcalloc(1, sizeof(*list));
	//rwlock_init(&list->lock);

	return list;
}

void cpair_list_free(struct cpair_list *list)
{
	xfree(list);
}

void cpair_list_add(struct cpair_list *list, struct cpair *pair)
{
	if (list->head == NULL) {
		list->head = pair;
		list->last = pair;
	} else {
		pair->list_prev = list->last;
		list->last->list_next = pair;
		list->last = pair;
	}
}

void cpair_list_remove(struct cpair_list *list, struct cpair *pair)
{
	if (list->head == pair)
		list->head = pair->list_next;

	if (pair->list_next)
		pair->list_next->list_prev = pair->list_prev;

	if (pair->list_prev)
		pair->list_prev->list_next = pair->list_next;

}

/*
 * free a cpair&node
 *
 * REQUIRES:
 * a) hashtable array lock on this cpair solt
 */
void _free_cpair(struct cache_file *cf, struct cpair *cp)
{
	struct cache *c = cf->cache;

	mutex_lock(&cf->mtx);
	cf->cache->cp_count--;
	mutex_unlock(&cf->mtx);

	quota_remove(c->quota, cp->attr.nodesz);
	cf->tree_opts->free_node(cp->v);
	mutex_destroy(&cp->mtx);
	xfree(cp);
}

static inline void _cf_clock_write_lock(struct cache_file *cf)
{
	mutex_lock(&cf->mtx);
	rwlock_write_lock(&cf->clock_lock, &cf->mtx);
	mutex_unlock(&cf->mtx);
}

static inline void _cf_clock_write_unlock(struct cache_file *cf)
{
	mutex_lock(&cf->mtx);
	rwlock_write_unlock(&cf->clock_lock);
	mutex_unlock(&cf->mtx);
}

static inline void _cf_clock_read_lock(struct cache_file *cf)
{
	mutex_lock(&cf->mtx);
	rwlock_read_lock(&cf->clock_lock, &cf->mtx);
	mutex_unlock(&cf->mtx);
}

static inline void _cf_clock_read_unlock(struct cache_file *cf)
{
	mutex_lock(&cf->mtx);
	rwlock_read_unlock(&cf->clock_lock);
	mutex_unlock(&cf->mtx);
}

/* try to evict(free) a pair */
void _try_evict_pair(struct cache_file *cf, struct cpair *p)
{
	int is_dirty;
	struct tree_operations *tree_opts = p->cf->tree_opts;

	if (!rwlock_users(&p->value_lock)) {
		/* barriered by key lock */
		xtable_slot_locked(cf->xtable, p);

		/* write dirty to disk */
		rwlock_write_lock(&p->disk_lock, &p->mtx);
		is_dirty = tree_opts->node_is_dirty(p->v);
		if (is_dirty) {
			tree_opts->flush_node(cf->fd, cf->hdr, p->v);
			tree_opts->node_set_nondirty(p->v);
		}
		rwlock_write_unlock(&p->disk_lock);

		/* remove from list */
		_cf_clock_write_lock(cf);
		cpair_list_remove(cf->clock, p);
		xtable_remove(cf->xtable, p);
		_cf_clock_write_unlock(cf);

		xtable_slot_unlocked(cf->xtable, p);

		/* here, none can grant the pair */
		_free_cpair(cf, p);
	}
}

/* pick one pair from clock list */
void _run_eviction(struct cache *c)
{
	struct cpair *cur;
	struct cpair *nxt;
	/* TODO (BohuTANG) : for all cache files */
	struct cache_file *cf;

	mutex_lock(&c->mtx);
	cf = c->cf_first;
	mutex_unlock(&c->mtx);
	if (!cf)
		return;

	/* barriered by clock READ lock */
	_cf_clock_read_lock(cf);
	cur = cf->clock->head;
	_cf_clock_read_unlock(cf);

	if (!cur)
		return;

	while (quota_highwater(c->quota) && cur) {
		int users = 0;

		/* barriered by clock READ lock */
		_cf_clock_read_lock(cf);
		users = rwlock_users(&cur->value_lock);
		nxt = cur->list_next;
		_cf_clock_read_unlock(cf);

		if (users == 0) {
			_try_evict_pair(cf, cur);
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

struct cache *cache_new(struct env *e)
{
	struct cache *c;
	double hw_percent = 0.0;

	c = xcalloc(1, sizeof(*c));
	c->e = e;
	mutex_init(&c->mtx);
	c->c_kibbutz = kibbutz_new(2);
	hw_percent = (double)(e->cache_high_watermark) / 100;
	c->quota = quota_new(e->cache_limits_bytes, hw_percent);

	ngettime(&c->last_checkpoint);
	c->flusher = cron_new(flusher_cb, e->cache_flush_period_ms);
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

void _cpair_insert(struct cache_file *cf, struct cpair *p)
{
	cpair_list_add(cf->clock, p);
	xtable_add(cf->xtable, p);
	cf->cache->cp_count++;
}

/* make room for writes */
void _make_room(struct cache *c)
{
	/* check cache limits, if reaches we should slow down */
	int allow_delay = 1;

	while (1) {
		int must = quota_fullwater(c->quota);
		int need = quota_highwater(c->quota);

		if (must) {
			cron_signal(c->flusher);
			quota_check_maybe_wait(c->quota);
		} else if (allow_delay && need) {
			/* slow down */
			usleep(100);
			allow_delay = 0;
		} else {
			break;
		}
	}
}

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
                      void **n,
                      enum lock_type locktype)
{
	struct cpair *p;
	struct cache *c = cf->cache;

TRY_PIN:
	/* make room for me, please */
	_make_room(c);

	/* barriered by key locked */
	struct cpair cptmp = {.k = k};
	xtable_slot_locked(cf->xtable, &cptmp);
	p = xtable_find(cf->xtable, &cptmp);
	if (p) {
		if (locktype != L_READ) {
			if (rwlock_users(&p->value_lock)) {
				xtable_slot_unlocked(cf->xtable, &cptmp);
				goto TRY_PIN;
			}
			rwlock_write_lock(&p->value_lock, &p->mtx);
		} else {
			if (rwlock_blocked_writers(&p->value_lock)) {
				xtable_slot_unlocked(cf->xtable, &cptmp);
				goto TRY_PIN;
			}
			rwlock_read_lock(&p->value_lock, &p->mtx);
		}

		nassert(p->pintype == L_NONE);
		*n = p->v;
		p->pintype = locktype;
		xtable_slot_unlocked(cf->xtable, &cptmp);

		return NESS_OK;
	}

	p = xtable_find(cf->xtable, &cptmp);
	if (p) {
		/*
		 * if we go here, means that someone got before us
		 * try pin again
		 */
		xtable_slot_unlocked(cf->xtable, p);
		goto TRY_PIN;
	}

	struct tree_operations *tree_opts = cf->tree_opts;
	int r = tree_opts->fetch_node(cf->fd, cf->hdr, k, n);

	if (r != NESS_OK) {
		__PANIC("fetch node from disk error, nid [%" PRIu64 "], errno %d",
		        k,
		        r);
		goto ERR;
	}

	p = cpair_new();
	cpair_init(p, k, *n, cf);

	/*
	 * add to cache list
	 * barriered by clock WRITE lock
	 */
	_cf_clock_write_lock(cf);
	_cpair_insert(cf, p);
	_cf_clock_write_unlock(cf);

	if (locktype != L_READ)
		rwlock_write_lock(&p->value_lock, &p->mtx);
	else
		rwlock_read_lock(&p->value_lock, &p->mtx);

	p->pintype = locktype;
	tree_opts->cache_put(*n, p);
	xtable_slot_unlocked(cf->xtable, p);

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int cache_put_and_pin(struct cache_file *cf, NID k, void *v)
{
	struct cpair *p;
	struct cache *c = cf->cache;
	struct tree_operations *tree_opts = cf->tree_opts;

	_make_room(c);
	p = cpair_new();

	/* default lock type is L_WRITE */
	rwlock_write_lock(&p->value_lock, &p->mtx);
	cpair_init(p, k, v, cf);
	p->pintype = L_WRITE;

	/* add to cache list, barriered by clock WRITE lock */
	_cf_clock_write_lock(cf);
	_cpair_insert(cf, p);
	_cf_clock_write_unlock(cf);
	tree_opts->cache_put(v, p);

	return NESS_OK;
}

void cache_unpin(struct cache_file *cf, struct cpair *p)
{
	int delta = 0;
	struct cache *c = cf->cache;

	nassert(p);
	nassert(p->pintype != L_NONE);

	int nodesz = (int)cf->tree_opts->node_size(p->v);
	xtable_slot_locked(cf->xtable, p);
	if (p->pintype == L_WRITE) {
		mutex_lock(&cf->mtx);
		p->pintype = L_NONE;
		delta = (int)(nodesz - p->attr.nodesz);
		p->attr.nodesz = nodesz;
		mutex_unlock(&cf->mtx);

		rwlock_write_unlock(&p->value_lock);
	} else {
		p->pintype = L_NONE;
		rwlock_read_unlock(&p->value_lock);
	}

	xtable_slot_unlocked(cf->xtable, p);
	if (delta > 0)
		quota_add(c->quota, delta);
	else
		quota_remove(c->quota, delta);
}

int cache_file_remove(struct cache *c, int filenum)
{
	(void)c;
	(void)filenum;

	return (-1);
}

struct cache_file *cache_file_create(struct cache *c,
                                     int fd,
                                     void *hdr,
                                     struct tree_operations *tree_opts)
{
	struct cache_file *cf;

	cf = xcalloc(1, sizeof(*cf));
	cf->cache = c;
	cf->fd = fd;
	cf->hdr = hdr;
	cf->tree_opts = tree_opts;
	cf->filenum = atomic32_increment(&c->filenum);

	mutex_init(&cf->mtx);
	ness_rwlock_init(&cf->clock_lock);
	cf->clock = cpair_list_new();
	cf->xtable = xtable_new(PAIR_LIST_SIZE, cache_xtable_hash_func, cache_xtable_compare_func);

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

void cache_file_free(struct cache_file *cf)
{
	xtable_free(cf->xtable);
	_cf_clock_read_lock(cf);
	cpair_list_free(cf->clock);
	cache_file_remove(cf->cache, cf->filenum);
	_cf_clock_read_unlock(cf);
	mutex_destroy(&cf->mtx);
	xfree(cf);
}

void cache_file_flush_dirty_nodes(struct cache_file *cf)
{
	int r;
	struct cpair *cur;
	struct cpair *nxt;
	struct tree_operations *tree_opts = cf->tree_opts;

	cur = cf->clock->head;
	while (cur) {
		nxt = cur->list_next;
		if (tree_opts->node_is_dirty(cur->v)) {
			xtable_slot_locked(cf->xtable, cur);
			r = tree_opts->flush_node(cf->fd, cf->hdr, cur->v);
			xtable_slot_unlocked(cf->xtable, cur);
			if (r != NESS_OK)
				__PANIC("serialize node to disk error, fd [%d]", cf->fd);
			tree_opts->node_set_nondirty(cur->v);
		}
		_free_cpair(cf, cur);
		cur = nxt;
	}

	r = tree_opts->flush_hdr(cf->fd, cf->hdr);
	if (r != NESS_OK)
		__PANIC("serialize hdr[%d] to disk error", cf->fd);
}

void cache_file_flush_hdr(struct cache_file *cf)
{
	int r;
	struct tree_operations *tree_opts = cf->tree_opts;

	r = tree_opts->flush_hdr(cf->fd, cf->hdr);
	if (r != NESS_OK)
		__PANIC("serialize hdr[%d] to disk error", cf->fd);
}

void cache_free(struct cache *c)
{
	/* first to stop background threads */
	kibbutz_free(c->c_kibbutz);
	cron_free(c->flusher);
	quota_free(c->quota);
	mutex_destroy(&c->mtx);
	xfree(c);
}
