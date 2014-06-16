/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

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
	mutex_init(&cp->mtx);
	ness_rwlock_init(&cp->disk_lock);
	ness_rwlock_init(&cp->value_lock);

	return cp;
}

void cpair_init(struct cpair *cp, struct node *value, struct cache_file *cf)
{
	cp->k = value->nid;
	cp->cf = cf;
	cp->v = value;
	value->cpair = cp;
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

	hash = (pair->v->nid & (PAIR_LIST_SIZE - 1));
	pair->hash_next = table->tables[hash];
	table->tables[hash] = pair;
}

void cpair_htable_remove(struct cpair_htable *table, struct cpair *pair)
{
	int hash;

	hash = (pair->v->nid & (PAIR_LIST_SIZE - 1));
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
void _free_cpair(struct cache *c, struct cpair *cp)
{
	mutex_lock(&c->mtx);
	c->cp_count--;
	c->cache_size -= cp->v->attr.newsz;
	mutex_unlock(&c->mtx);

	node_free(cp->v);
	xfree(cp);
}

static inline void _cache_clock_write_lock(struct cache *c)
{
	mutex_lock(&c->mtx);
	rwlock_write_lock(&c->clock_lock, &c->mtx);
	mutex_unlock(&c->mtx);
}

static inline void _cache_clock_write_unlock(struct cache *c)
{
	mutex_lock(&c->mtx);
	rwlock_write_unlock(&c->clock_lock);
	mutex_unlock(&c->mtx);
}

static inline void _cache_clock_read_lock(struct cache *c)
{
	mutex_lock(&c->mtx);
	rwlock_read_lock(&c->clock_lock, &c->mtx);
	mutex_unlock(&c->mtx);
}

static inline void _cache_clock_read_unlock(struct cache *c)
{
	mutex_lock(&c->mtx);
	rwlock_read_unlock(&c->clock_lock);
	mutex_unlock(&c->mtx);
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

	if (!rwlock_users(&p->value_lock)) {
		/* barriered by key lock */
		cpair_locked_by_key(c->table, p->k);

		/* write dirty to disk */
		rwlock_write_lock(&p->disk_lock, &p->mtx);
		is_dirty = node_is_dirty(p->v);
		if (is_dirty) {
			tcb->flush_node(tree, p->v);
			node_set_nondirty(p->v);
		}
		rwlock_write_unlock(&p->disk_lock);

		/* remove from list */
		_cache_clock_write_lock(c);
		cpair_list_remove(c->clock, p);
		cpair_htable_remove(c->table, p);
		_cache_clock_write_unlock(c);

		cpair_unlocked_by_key(c->table, p->k);

		/* here, none can grant the pair */
		_free_cpair(c, p);
	}
}

void _cache_dump(struct cache *c, const char *msg)
{
	struct cpair *cur;
	uint64_t all_size = 0UL;

	cur = c->clock->head;
	printf("----%s: cache dump:\n", msg);
	while (cur) {
		int children = cur->v->height > 0 ? cur->v->u.n.n_children : 0;
		printf("\t nid[%" PRIu64 "]"
		       " \tisroot[%d]"
		       " \theight[%d]"
		       " \tchildren[%d]"
		       " \tnodesz[%d]MB"
		       " \tdirty[%d]"
		       " \tnum_readers[%d]"
		       " \tnum_writers[%d]\n",
		       cur->v->nid,
		       cur->v->isroot == 1,
		       cur->v->height,
		       children,
		       (int)(cur->v->attr.newsz / (1024 * 1024)),
		       node_is_dirty(cur->v),
		       cur->value_lock.reader,
		       cur->value_lock.writer
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

	/* barriered by clock READ lock */
	_cache_clock_read_lock(c);
	cur = c->clock->head;
	_cache_clock_read_unlock(c);

	if (!cur)
		return;

	while (_need_evict(c) && cur) {
		int isroot = 0;
		int users = 0;

		/* barriered by clock READ lock */
		_cache_clock_read_lock(c);
		isroot = cur->v->isroot;
		users = rwlock_users(&cur->value_lock);
		nxt = cur->list_next;
		_cache_clock_read_unlock(c);

		if ((users == 0) && (isroot == 0)) {
			_try_evict_pair(c, cur);
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

struct cache *cache_new(struct options *opts) {
	struct cache *c;

	c = xcalloc(1, sizeof(*c));
	mutex_init(&c->mtx);
	mutex_init(&c->makeroom_mtx);
	cond_init(&c->wait_makeroom);
	ness_rwlock_init(&c->clock_lock);

	c->clock = cpair_list_new();
	c->table = cpair_htable_new();
	c->c_kibbutz = kibbutz_new(2);

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

	/* barriered by key locked */
	cpair_locked_by_key(c->table, k);
	p = cpair_htable_find(c->table, k);
	if (p) {
		if (locktype != L_READ) {
			if (rwlock_users(&p->value_lock)) {
				cpair_unlocked_by_key(c->table, k);
				goto TRY_PIN;
			}
			rwlock_write_lock(&p->value_lock, &p->mtx);
		} else {
			if (rwlock_blocked_writers(&p->value_lock)) {
				cpair_unlocked_by_key(c->table, k);
				goto TRY_PIN;
			}
			rwlock_read_lock(&p->value_lock, &p->mtx);
		}

		nassert(p->v->pintype == L_NONE);
		*n = p->v;
		(*n)->pintype = locktype;
		cpair_unlocked_by_key(c->table, k);

		return NESS_OK;
	}

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

	/*
	 * add to cache list
	 * barriered by clock WRITE lock
	 */
	_cache_clock_write_lock(c);
	_cache_insert(c, p);
	_cache_clock_write_unlock(c);

	if (locktype != L_READ)
		rwlock_write_lock(&p->value_lock, &p->mtx);
	else
		rwlock_read_lock(&p->value_lock, &p->mtx);

	(*n)->pintype = locktype;
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
	struct tree *t = (struct tree*)cf->args;

	/* make room for me, please */
	_make_room(c);

	next_nid = hdr_next_nid(t);
	if (height == 0) {
		new_node = leaf_alloc_empty(next_nid);
		status_increment(&t->status->tree_leaf_nums);
	} else {
		new_node = nonleaf_alloc_empty(next_nid, height, children);
		status_increment(&t->status->tree_nonleaf_nums);
	}

	p = cpair_new();
	cpair_init(p, new_node, cf);

	/* default lock type is L_WRITE */

	rwlock_write_lock(&p->value_lock, &p->mtx);
	*n = new_node;
	(*n)->pintype = L_WRITE;

	/* add to cache list, barriered by clock WRITE lock */
	_cache_clock_write_lock(c);
	_cache_insert(c, p);
	_cache_clock_write_unlock(c);

	return NESS_OK;
}

void cache_unpin(struct cache_file *cf, struct node *n)
{
	NID k = n->nid;
	struct cpair *p = n->cpair;
	struct cache *c = cf->cache;

	nassert(p);
	nassert(n->pintype != L_NONE);

	cpair_locked_by_key(c->table, k);
	if (n->pintype == L_WRITE) {
		mutex_lock(&c->mtx);
		n->pintype = L_NONE;
		n->attr.oldsz = n->attr.newsz;
		n->attr.newsz = node_size(n);
		c->cache_size += (n->attr.newsz - n->attr.oldsz);
		mutex_unlock(&c->mtx);

		rwlock_write_unlock(&p->value_lock);
	} else {
		n->pintype = L_NONE;
		rwlock_read_unlock(&p->value_lock);
	}

	cpair_unlocked_by_key(c->table, k);
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

void cache_flush_all_dirty_nodes(struct cache *c)
{
	int r;
	struct cpair *cur;
	struct cpair *nxt;

	cur = c->clock->head;
	while (cur) {
		nxt = cur->list_next;
		if (node_is_dirty(cur->v)) {
			NID k = cur->v->nid;
			struct tree *tree;
			struct tree_callback *tcb;

			cpair_locked_by_key(c->table, k);
			tree = (struct tree*)cur->cf->args;
			tcb = cur->cf->tcb;
			r = tcb->flush_node(tree, cur->v);
			cpair_unlocked_by_key(c->table, k);
			if (r != NESS_OK)
				__PANIC("serialize node to disk error, fd [%d]", tree->fd);
			node_set_nondirty(cur->v);
		}
		_free_cpair(c, cur);
		cur = nxt;
	}

	struct cache_file *cf_cur;
	struct cache_file *cf_nxt;

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
	/* first to stop background threads */
	kibbutz_free(c->c_kibbutz);
	cron_free(c->flusher);
	cache_flush_all_dirty_nodes(c);

	cpair_htable_free(c->table);
	cpair_list_free(c->clock);
	xfree(c);
}
