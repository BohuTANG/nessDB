/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_SKIPLIST_H_
#define nessDB_SKIPLIST_H_

#include "xtypes.h"
#include "internal.h"
#include "mempool.h"

/**
 * @file skiplist.h
 * @brief skiplist definitions
 *
 * skiplist is the fundamental sort-structure with iterator(thread-safe).
 * it's used in innernode's msgbuf and leafnode's msgbuf.
 *
 */

/* compare function */
typedef int (*SKIPLIST_COMPARE_CALLBACK)(void *, void *, struct cmp_extra *);

struct skiplist_iter {
	struct skipnode *node;
	struct skiplist *list;
};

#define SLOTS_SIZE (4)

struct skipnode {
	int size;
	int used;
	void **keys;
	struct skipnode *next[1];
};

struct skiplist {
	int count;
	int unique;
	int height;
	struct skipnode *header;
	struct mempool *mpool;
	SKIPLIST_COMPARE_CALLBACK compare_cb;
};

struct skiplist *skiplist_new(SKIPLIST_COMPARE_CALLBACK compare_cb);

void skiplist_put(struct skiplist *sl, void *key);
struct skipnode *skiplist_find_less_than(struct skiplist *sl, void *key);
struct skipnode *skiplist_find_last(struct skiplist *sl);
void skiplist_free(struct skiplist *sl);

/* skiplsit iterator */
void skiplist_iter_init(struct skiplist_iter *iter, struct skiplist *list);
int skiplist_iter_valid(struct skiplist_iter *iter);
void skiplist_iter_next(struct skiplist_iter *iter);
void skiplist_iter_prev(struct skiplist_iter *iter);
void skiplist_iter_seek(struct skiplist_iter *iter, void *key);
void skiplist_iter_seektofirst(struct skiplist_iter *iter);
void skiplist_iter_seektolast(struct skiplist_iter *iter);

#endif /* _nessDB_SKIPLIST_H_ */
