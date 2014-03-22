/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "atomic.h"
#include "skiplist.h"
#include "debug.h"

/**
 * @file skiplist.c
 * @brief skiplist
 *
 * skiplist:
 *	- support multi-version (because there is no remove)
 *	- read is lock-free(with Memory Barrier: Acquire & Release)
 *	- write should need a latch to protect.
 */

#define SKIPLIST_MAX_LEVEL	(12)

static inline struct skipnode *_get_next(struct skipnode *x, int height)
{
	struct skipnode *next;

	next = x->next[height];
	memory_barrier();

	return next;
}

static inline void _set_next(struct skipnode **a, struct skipnode *b)
{
	release_store((void**)a, (void*)b);
}

static inline int _get_height(struct skiplist *sl)
{
	int h;

	h = sl->height;
	memory_barrier();

	return h;
}

static inline void _set_height(int *a, int *b)
{
	memory_barrier();
	*a = *b;
}

struct skipnode *skiplist_find_greater_or_equal(struct skiplist *sl,
		void *key,
		struct skipnode **prev)
{
	int level;
	register struct skipnode *x;
	register struct skipnode *next;

	level = _get_height(sl) - 1;
	x = sl->header;
	while (1) {
		next = x->next[level];
		if ((next != NULL) && (sl->compare_cb(next->key, key) < 0)) {
			x = next;
		} else {
			if (prev)
				prev[level] = x;

			if (level == 0)
				return next;
			else
				level--;
		}
	}
}

int _rand_height()
{
	int height = 1;
	int branching = 4;

	while(((rand() % branching) == 0) && (height < SKIPLIST_MAX_LEVEL))
		height++;

	return height;
}

struct skipnode *_new_node(struct skiplist *sl, int height)
{
	uint32_t sizes;
	struct skipnode *n;

	sizes = (sizeof(*n) + (height - 1) * sizeof(void*));
	n = (struct skipnode*)mempool_alloc_aligned(sl->mpool, sizes);
	memset(n, 0, sizes);

	return n;
}


struct skiplist *skiplist_new(SKIPLIST_COMPARE_CALLBACK compare_cb)
{
	struct skiplist *sl;

	sl = xcalloc(1, sizeof(*sl));
	sl->mpool = mempool_new();
	sl->header = _new_node(sl, SKIPLIST_MAX_LEVEL);
	sl->compare_cb = compare_cb;
	sl->height = 1;

	return sl;
}

/*
 * REQUIRES:
 *	  - cannot put duplicate key, deal it in judge_callback
 */
void skiplist_put(struct skiplist *sl, void *key)
{
	int i;
	int height;

	struct skipnode *x;
	struct skipnode *prev[SKIPLIST_MAX_LEVEL];

	memset(prev, 0, sizeof(struct skipnode*) * SKIPLIST_MAX_LEVEL);
	x = skiplist_find_greater_or_equal(sl, key, prev);
	height = _rand_height();

	/* init prev arrays */
	if (height > _get_height(sl)) {
		for (i = _get_height(sl); i < height; i++)
			prev[i] = sl->header;
		_set_height(&sl->height, &height);
	}

	x = _new_node(sl, height);
	x->key = key;

	for (i = 0; i < height; i++) {
		_set_next(&x->next[i], prev[i]->next[i]);
		_set_next(&prev[i]->next[i], x);
	}
	sl->count++;
}

int skiplist_contains(struct skiplist *sl, void *key)
{
	struct skipnode *x;

	x = skiplist_find_greater_or_equal(sl, key, NULL);
	if (x != NULL && sl->compare_cb(x->key, key) == 0)
		return NESS_OK;
	else
		return NESS_ERR;
}

struct skipnode *skiplist_find_less_than(struct skiplist *sl, void *key)
{
	int height;
	struct skipnode *x;
	
	height = _get_height(sl) - 1;
	x = sl->header;
	while (1) {
		struct skipnode *next = _get_next(x, height);;

		if (next == NULL || sl->compare_cb(next->key, key) >= 0) {
			if (height == 0)
				return x;
			else
				height--;
		} else {
			x = next;
		}
	}
}

struct skipnode *skiplist_find_last(struct skiplist *sl)
{
	int level;
	struct skipnode *x;

	x = sl->header;
	level = _get_height(sl) - 1;
	while (1) {
		struct skipnode *next;

		next = _get_next(x, level);
		if (next == NULL) {
			if (level == 0)
				return x;
			else
				level--;
		} else {
			x = next;
		}
	}
}

void skiplist_free(struct skiplist *sl)
{
	mempool_free(sl->mpool);
	xfree(sl);
}

/*******************************************************
 * skiplsit iterator (thread-safe)
*******************************************************/

/* init */
void skiplist_iter_init(struct skiplist_iter *iter, struct skiplist *list)
{
	iter->node = NULL;
	iter->list = list;
}

/* valid */
int skiplist_iter_valid(struct skiplist_iter *iter)
{
	return (iter->node != NULL);
}

/* next */
void skiplist_iter_next(struct skiplist_iter *iter)
{
	struct skipnode *n;

	nassert(skiplist_iter_valid(iter));

	n = _get_next(iter->node, 0);
	iter->node = n;
}

/* prev */
void skiplist_iter_prev(struct skiplist_iter *iter)
{
	struct skipnode *n;

	nassert(skiplist_iter_valid(iter));
	n = skiplist_find_less_than(iter->list, iter->node->key);
	if (n == iter->list->header)
		n = NULL;

	iter->node = n;
}

/* seek */
void skiplist_iter_seek(struct skiplist_iter *iter, void *key)
{
	struct skipnode *n;

	n = skiplist_find_greater_or_equal(iter->list, key, NULL);
	iter->node = n;
}

/* seek to first */
void skiplist_iter_seektofirst(struct skiplist_iter *iter)
{
	struct skipnode *n;

	n = _get_next(iter->list->header, 0);
	iter->node = n;
}

/* seek to last */
void skiplist_iter_seektolast(struct skiplist_iter *iter)
{
	struct skipnode *n;

	n = skiplist_find_last(iter->list);
	if (n == iter->list->header)
		n = NULL;
	iter->node = n;
}
