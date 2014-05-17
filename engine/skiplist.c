/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "skiplist.h"
#include "debug.h"

/**
 * @file skiplist.c
 * @brief skiplist
 *
 * skiplist:
 *	- support multi-version
 */

#define SKIPLIST_MAX_LEVEL	(12)

static inline int _sl_cmp(struct skiplist *sl, void *a, void *b, struct cmp_extra *extra)
{
	int r = sl->compare_cb(a, b);

	if (r == 0 && extra)
		extra->exists = 1;

	return r;
}

struct skipnode *skiplist_find_greater_or_equal(struct skiplist *sl,
                void *key,
                struct skipnode **prev,
                struct cmp_extra *extra) {
	register int level;
	register struct skipnode *x;
	register struct skipnode *next;

	level = sl->height - 1;
	x = sl->header;
	while (1) {
		next = x->next[level];
		if ((next != NULL) &&
		    (_sl_cmp(sl, next->key, key, extra) < 0)) {
			x = next;
		} else {
			if (prev)
				prev[level] = x;

			if (level > 0)
				level--;
			else
				return next;
		}
	}
}

int _rand_height()
{
	int height = 1;
	int branching = 4;

	while (((rand() % branching) == 0) && (height < SKIPLIST_MAX_LEVEL))
		height++;

	return height;
}

struct skipnode *_new_node(struct skiplist *sl, int height) {
	uint32_t sizes;
	struct skipnode *n;

	sizes = (sizeof(*n) + (height - 1) * sizeof(void*));
	n = (struct skipnode*)mempool_alloc_aligned(sl->mpool, sizes);
	memset(n, 0, sizes);
	n->key = (void*)mempool_alloc_aligned(sl->mpool, sizeof(void*));

	return n;
}


struct skiplist *skiplist_new(SKIPLIST_COMPARE_CALLBACK compare_cb) {
	struct skiplist *sl;

	sl = xcalloc(1, sizeof(*sl));
	sl->mpool = mempool_new();
	sl->header = _new_node(sl, SKIPLIST_MAX_LEVEL);
	sl->compare_cb = compare_cb;
	sl->height = 1;

	return sl;
}

/*
 * make a room for putting
 * after the function, make sure to set the key
 * RETURN:
 *	0x00) if newroom is true, return the new skipnode, we should set the key
 *	0x01) if newroom is false, return the skipnode who has the existent key
 */
int skiplist_makeroom(struct skiplist *sl, void *key, struct skipnode **refnode)
{
	int i;
	int height;
	int newroom = 0;

	struct skipnode *x;
	struct skipnode *prev[SKIPLIST_MAX_LEVEL];
	struct cmp_extra extra = {.exists = 0};

	memset(prev, 0, sizeof(struct skipnode*) * SKIPLIST_MAX_LEVEL);
	x = skiplist_find_greater_or_equal(sl, key, prev, &extra);

	/* new key */
	if (!extra.exists) {
		height = _rand_height();

		/* init prev arrays */
		if (height > sl->height) {
			for (i = sl->height; i < height; i++)
				prev[i] = sl->header;
			sl->height = height;
		}

		x = _new_node(sl, height);
		for (i = 0; i < height; i++) {
			x->next[i] = prev[i]->next[i];
			prev[i]->next[i] = x;
		}
		*refnode = x;
		sl->count++;
		newroom = 1;
	} else {
		*refnode = x;
		newroom = 0;
	}

	return newroom;
}

void skiplist_put(struct skiplist *sl, void *key)
{
	int newroom;
	struct skipnode *node;

	newroom = skiplist_makeroom(sl, key, &node);
	if (newroom)
		node->key = key;
}

void *skiplist_find(struct skiplist *sl, void *key)
{
	struct skipnode *x = NULL;
	struct cmp_extra extra = {.exists = 0 };

	x = skiplist_find_greater_or_equal(sl, key, NULL, &extra);
	if (extra.exists)
		return x->key;

	return NULL;
}

struct skipnode *skiplist_find_less_than(struct skiplist *sl, void *key) {
	int height;
	struct skipnode *x;

	height = sl->height - 1;
	x = sl->header;
	while (1) {
		struct skipnode *next = x->next[height];

		if (next == NULL || _sl_cmp(sl, next->key, key, NULL) >= 0) {
			if (height == 0)
				return x;
			else
				height--;
		} else {
			x = next;
		}
	}
}

struct skipnode *skiplist_find_last(struct skiplist *sl) {
	int level;
	struct skipnode *x;

	x = sl->header;
	level = sl->height - 1;
	while (1) {
		struct skipnode *next;

		next = x->next[level];
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

	n = iter->node->next[0];
	iter->node = n;
}

/*
 * instead of using explicit 'prev' links
 * we just search for the last node less than the key
 * and it's lock free for reading
 * otherwise, the 'prev' links update is more complex to be atomic
 */
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

	n = skiplist_find_greater_or_equal(iter->list, key, NULL, NULL);
	iter->node = n;
}

/* seek to first */
void skiplist_iter_seektofirst(struct skiplist_iter *iter)
{
	struct skipnode *n;

	n = iter->list->header->next[0];
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
