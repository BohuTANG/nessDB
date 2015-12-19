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

void cpair_attr_update(struct cpair *cp, struct cpair_attr *attr)
{
	xmemcpy(&cp->attr, attr, sizeof(*attr));
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
	if (cp) {
		quota_remove(c->quota, cp->attr.nodesz);
		quota_remove(c->quota, sizeof(*cp));
		cf->tree_opts->free_node(cp->v);
		mutex_destroy(&cp->mtx);
		cp->v = NULL;
		xfree(cp);
		cf->cache->cp_count--;
	}
	mutex_unlock(&cf->mtx);
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
	int is_root;
	int removed = 0;
	int hash = p->k;
	struct tree_operations *tree_opts;

	tree_opts = p->cf->tree_opts;
	is_root = tree_opts->node_is_root(p->v);
	if (is_root)
		return;

	is_dirty = tree_opts->node_is_dirty(p->v);
	if (is_dirty) {
		xtable_slot_locked(cf->xtable, hash);
		rwlock_write_lock(&p->disk_lock, &p->mtx);
		xtable_slot_unlocked(cf->xtable, hash);

		tree_opts->flush_node(cf->fd, cf->hdr, p->v);
		tree_opts->node_set_nondirty(p->v);

		xtable_slot_locked(cf->xtable, hash);
		rwlock_write_unlock(&p->disk_lock);
		xtable_slot_unlocked(cf->xtable, hash);
	}

	xtable_slot_locked(cf->xtable, hash);
	if (rwlock_users(&p->value_lock) == 0) {
		/* remove from list */
		_cf_clock_write_lock(cf);
		cpair_list_remove(cf->clock, p);
		xtable_remove(cf->xtable, p);
		removed = 1;
		_cf_clock_write_unlock(cf);
	}
	xtable_slot_unlocked(cf->xtable, hash);

	if (removed) {
		nassert(rwlock_users(&p->value_lock) == 0);
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

	while ((quota_state(c->quota) != QUOTA_STATE_NONE) && cur) {
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
	quota_signal(c->quota);
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

	c = xcalloc(1, sizeof(*c));
	c->e = e;
	mutex_init(&c->mtx);
	c->c_kibbutz = kibbutz_new(2);
	c->quota = quota_new(e->cache_limits_bytes);

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
		QUOTA_STATE state = quota_state(c->quota);

		switch (state) {
		case QUOTA_STATE_MUST_EVICT:
			cron_signal(c->flusher);
			quota_wait(c->quota);
			break;
		case QUOTA_STATE_NEED_EVICT:
			if (allow_delay) {
				usleep(100);
				allow_delay = 0;
			}
			break;
		case QUOTA_STATE_NONE:
			return;
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
	struct cpair cptmp = {.k = k};

TRY_PIN:
	/* make room for me, please */
	_make_room(c);

	/* barriered by key locked */
	xtable_slot_locked(cf->xtable, k);
	p = xtable_find(cf->xtable, &cptmp);
	if (p) {
		if (locktype != L_READ) {
			if (rwlock_users(&p->value_lock)) {
				xtable_slot_unlocked(cf->xtable, k);
				goto TRY_PIN;
			}
			rwlock_write_lock(&p->value_lock, &p->mtx);
		} else {
			if (rwlock_blocked_writers(&p->value_lock)) {
				xtable_slot_unlocked(cf->xtable, k);
				goto TRY_PIN;
			}
			rwlock_read_lock(&p->value_lock, &p->mtx);
		}

		nassert(p->pintype == L_NONE);
		*n = p->v;
		p->pintype = locktype;
		xtable_slot_unlocked(cf->xtable, k);

		return NESS_OK;
	}

	p = xtable_find(cf->xtable, &cptmp);
	if (p) {
		/*
		 * if we go here, means that someone got before us
		 * try pin again
		 */
		xtable_slot_unlocked(cf->xtable, k);
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
	quota_add(c->quota, sizeof(*p));

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
	xtable_slot_unlocked(cf->xtable, k);

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
	int hash = p->k;
	uint64_t old_nodesz;
	uint64_t new_nodesz;
	struct cache *c = cf->cache;

	nassert(p);
	nassert(p->pintype != L_NONE);

	xtable_slot_locked(cf->xtable, hash);
	old_nodesz = p->attr.nodesz;
	new_nodesz = cf->tree_opts->node_size(p->v);
	if (p->pintype == L_WRITE)
		rwlock_write_unlock(&p->value_lock);
	else
		rwlock_read_unlock(&p->value_lock);
	p->pintype = L_NONE;

	struct cpair_attr new_attr = {
		.nodesz = new_nodesz,
	};
	cpair_attr_update(p, &new_attr);
	xtable_slot_unlocked(cf->xtable, hash);

	quota_remove(c->quota, old_nodesz);
	quota_add(c->quota, new_nodesz);
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
	int hash;
	int removed = 0;
	struct cpair *cur;
	struct cpair *nxt;
	struct tree_operations *tree_opts = cf->tree_opts;

	cur = cf->clock->head;
	while (cur) {
		hash = cur->k;
		nxt = cur->list_next;
		if (tree_opts->node_is_dirty(cur->v)) {
			xtable_slot_locked(cf->xtable, hash);
			r = tree_opts->flush_node(cf->fd, cf->hdr, cur->v);
			xtable_slot_unlocked(cf->xtable, hash);
			if (r != NESS_OK)
				__PANIC("serialize node to disk error, fd [%d]", cf->fd);
			tree_opts->node_set_nondirty(cur->v);
		}

		xtable_slot_locked(cf->xtable, hash);
		if (rwlock_users(&cur->value_lock) == 0) {
			_cf_clock_write_lock(cf);
			cpair_list_remove(cf->clock, cur);
			xtable_remove(cf->xtable, cur);
			removed = 1;
			_cf_clock_write_unlock(cf);
		}
		xtable_slot_unlocked(cf->xtable, hash);

		if (removed)
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
