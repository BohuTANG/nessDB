/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "hdr.h"
#include "cache.h"

#define PAIR_LIST_SIZE (5<<10)

struct cpair_list {
	struct cpair *head;
	struct cpair *last;
};

struct cpair_htable {
	struct cpair **tables;
	ness_mutex_aligned_t *mutexes;	/* array lock */
};

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

struct cpair *cpair_new() {
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
struct cpair_list *cpair_list_new() {
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

/*******************************
 * hashtable
 ******************************/
struct cpair_htable *cpair_htable_new() {
	int i;
	struct cpair_htable *table;

	table = xcalloc(1, sizeof(*table));
	table->tables = xcalloc(PAIR_LIST_SIZE, sizeof(*table->tables));
	table->mutexes = xcalloc(PAIR_LIST_SIZE, sizeof(*table->mutexes));
	for (i = 0; i < PAIR_LIST_SIZE; i++) {
		mutex_init(&table->mutexes[i].aligned_mtx);
	}

	return table;
}

void cpair_htable_free(struct cpair_htable *table)
{
	xfree(table->tables);
	xfree(table->mutexes);
	xfree(table);
}

void cpair_htable_add(struct cpair_htable *table, struct cpair *pair)
{
	int hash;

	hash = (pair->k & (PAIR_LIST_SIZE - 1));
	pair->hash_next = table->tables[hash];
	table->tables[hash] = pair;
}

void cpair_htable_remove(struct cpair_htable *table, struct cpair *pair)
{
	int hash;

	hash = (pair->k & (PAIR_LIST_SIZE - 1));
	if (table->tables[hash] == pair) {
		table->tables[hash] = pair->hash_next;
	} else {
		struct cpair *curr = table->tables[hash];
		while (curr->hash_next != pair) {
			curr = curr->hash_next;
		}
		curr->hash_next = pair->hash_next;
	}
}

struct cpair *cpair_htable_find(struct cpair_htable *table, NID key) {
	int hash;
	struct cpair *curr;

	hash = (key & (PAIR_LIST_SIZE - 1));
	curr = table->tables[hash];
	while (curr) {
		if (curr->k == key) {
			return curr;
		}
		curr = curr->hash_next;
	}

	return NULL;
}

/*
 * free a cpair&node
 *
 * REQUIRES:
 * a) hashtable array lock on this cpair solt
 */
void _free_cpair(struct cache_file *cf, struct cpair *cp)
{
	mutex_lock(&cf->mtx);
	cf->cache->cp_count--;
	cf->cache->cache_size -= cp->attr.nodesz;
	mutex_unlock(&cf->mtx);

	cf->tcb->free_node_cb(cp->v);
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

static inline int _must_evict(struct cache *c)
{
	int must;

	mutex_lock(&c->mtx);
	must = ((c->cache_size > c->e->cache_limits_bytes));
	mutex_unlock(&c->mtx);

	return must;
}

static inline int _need_evict(struct cache *c)
{
	int need;

	mutex_lock(&c->mtx);
	need = (c->cache_size > (c->e->cache_limits_bytes *
	                         c->e->cache_high_watermark / 100));
	mutex_unlock(&c->mtx);

	return need;
}

/* try to evict(free) a pair */
void _try_evict_pair(struct cache_file *cf, struct cpair *p)
{
	int is_dirty;
	struct tree_callback *tcb = p->cf->tcb;

	if (!rwlock_users(&p->value_lock)) {
		/* barriered by key lock */
		cpair_locked_by_key(cf->table, p->k);

		/* write dirty to disk */
		rwlock_write_lock(&p->disk_lock, &p->mtx);
		is_dirty = tcb->node_is_dirty_cb(p->v);
		if (is_dirty) {
			tcb->flush_node_cb(cf->fd, cf->hdr, p->v);
			tcb->node_set_nondirty_cb(p->v);
		}
		rwlock_write_unlock(&p->disk_lock);

		/* remove from list */
		_cf_clock_write_lock(cf);
		cpair_list_remove(cf->clock, p);
		cpair_htable_remove(cf->table, p);
		_cf_clock_write_unlock(cf);

		cpair_unlocked_by_key(cf->table, p->k);

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
	struct cache_file *cf = c->cf_first;

	/* barriered by clock READ lock */
	_cf_clock_read_lock(cf);
	cur = cf->clock->head;
	_cf_clock_read_unlock(cf);

	if (!cur)
		return;

	while (_need_evict(c) && cur) {
		int users = 0;

		/* barriered by clock READ lock */
		_cf_clock_read_lock(cf);
		users = rwlock_users(&cur->value_lock);
		nxt = cur->list_next;
		_cf_clock_read_unlock(cf);

		if ((users == 0)) {
			_try_evict_pair(cf, cur);
			cond_signalall(&c->wait_makeroom);
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

struct cache *cache_new(struct env *e) {
	struct cache *c;

	c = xcalloc(1, sizeof(*c));
	mutex_init(&c->makeroom_mtx);
	cond_init(&c->wait_makeroom);

	c->c_kibbutz = kibbutz_new(2);

	c->e = e;
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
	cpair_htable_add(cf->table, p);
	cf->cache->cp_count++;
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
			__WARN("must evict, waiting..., cache limits [%" PRIu64 "]MB", c->cache_size / 1048576);
			cron_signal(c->flusher);
			cond_wait(&c->wait_makeroom, &c->makeroom_mtx);
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
	cpair_locked_by_key(cf->table, k);
	p = cpair_htable_find(cf->table, k);
	if (p) {
		if (locktype != L_READ) {
			if (rwlock_users(&p->value_lock)) {
				cpair_unlocked_by_key(cf->table, k);
				goto TRY_PIN;
			}
			rwlock_write_lock(&p->value_lock, &p->mtx);
		} else {
			if (rwlock_blocked_writers(&p->value_lock)) {
				cpair_unlocked_by_key(cf->table, k);
				goto TRY_PIN;
			}
			rwlock_read_lock(&p->value_lock, &p->mtx);
		}

		nassert(p->pintype == L_NONE);
		*n = p->v;
		p->pintype = locktype;
		cpair_unlocked_by_key(cf->table, k);

		return NESS_OK;
	}

	p = cpair_htable_find(cf->table, k);
	if (p) {
		/*
		 * if we go here, means that someone got before us
		 * try pin again
		 */
		cpair_unlocked_by_key(cf->table, k);
		goto TRY_PIN;
	}

	struct tree_callback *tcb = cf->tcb;
	int r = tcb->fetch_node_cb(cf->fd, cf->hdr, k, n);

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
	tcb->cache_put_cb(*n, p);
	cpair_unlocked_by_key(cf->table, k);

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int cache_put_and_pin(struct cache_file *cf, NID k, void *v)
{
	struct cpair *p;
	struct cache *c = cf->cache;
	struct tree_callback *tcb = cf->tcb;

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
	tcb->cache_put_cb(v, p);

	return NESS_OK;
}

void cache_unpin(struct cache_file *cf, struct cpair *p, struct cpair_attr attr)
{
	struct cache *c = cf->cache;

	nassert(p);
	nassert(p->pintype != L_NONE);

	NID k = p->k;
	cpair_locked_by_key(cf->table, k);
	if (p->pintype == L_WRITE) {
		mutex_lock(&cf->mtx);
		p->pintype = L_NONE;
		c->cache_size += (int)(attr.nodesz - p->attr.nodesz);
		p->attr = attr;
		mutex_unlock(&cf->mtx);

		rwlock_write_unlock(&p->value_lock);
	} else {
		p->pintype = L_NONE;
		rwlock_read_unlock(&p->value_lock);
	}

	cpair_unlocked_by_key(cf->table, k);
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
                                     struct tree_callback *tcb) {
	struct cache_file *cf;

	cf = xcalloc(1, sizeof(*cf));
	cf->cache = c;
	cf->tcb = tcb;
	cf->fd = fd;
	cf->hdr = hdr;
	cf->filenum = atomic32_increment(&c->filenum);

	mutex_init(&cf->mtx);
	ness_rwlock_init(&cf->clock_lock);
	cf->clock = cpair_list_new();
	cf->table = cpair_htable_new();

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
	cpair_htable_free(cf->table);
	cpair_list_free(cf->clock);
	cache_file_remove(cf->cache, cf->filenum);
	xfree(cf);
}

void cache_file_flush_dirty_nodes(struct cache_file *cf)
{
	int r;
	struct cpair *cur;
	struct cpair *nxt;
	struct tree_callback *tcb = cf->tcb;

	cur = cf->clock->head;
	while (cur) {
		nxt = cur->list_next;
		if (tcb->node_is_dirty_cb(cur->v)) {
			NID k = cur->k;

			cpair_locked_by_key(cf->table, k);
			r = tcb->flush_node_cb(cf->fd, cf->hdr, cur->v);
			cpair_unlocked_by_key(cf->table, k);
			if (r != NESS_OK)
				__PANIC("serialize node to disk error, fd [%d]", cf->fd);
			tcb->node_set_nondirty_cb(cur->v);
		}
		_free_cpair(cf, cur);
		cur = nxt;
	}

	r = tcb->flush_hdr_cb(cf->fd, cf->hdr);
	if (r != NESS_OK)
		__PANIC("serialize hdr[%d] to disk error", cf->fd);
}

void cache_file_flush_hdr(struct cache_file *cf)
{
	int r;
	struct tree_callback *tcb = cf->tcb;

	r = tcb->flush_hdr_cb(cf->fd, cf->hdr);
	if (r != NESS_OK)
		__PANIC("serialize hdr[%d] to disk error", cf->fd);
}

void cache_free(struct cache *c)
{
	/* first to stop background threads */
	kibbutz_free(c->c_kibbutz);
	cron_free(c->flusher);

	xfree(c);
}
