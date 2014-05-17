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
 */

/* compare function */
typedef int (*SKIPLIST_COMPARE_CALLBACK)(void *, void *);

struct skiplist_iter {
	struct skipnode *node;
	struct skiplist *list;
};

struct skipnode {
	void *key;
	struct skipnode *next[1];
};

struct skiplist {
	int count;
	int height;
	struct skipnode *header;
	struct mempool *mpool;
	SKIPLIST_COMPARE_CALLBACK compare_cb;
};

struct skiplist *skiplist_new(SKIPLIST_COMPARE_CALLBACK compare_cb);
int skiplist_makeroom(struct skiplist *, void *, struct skipnode **);
void skiplist_put(struct skiplist *, void *);
void *skiplist_find(struct skiplist *, void *);
struct skipnode *skiplist_find_less_than(struct skiplist *, void *);
struct skipnode *skiplist_find_last(struct skiplist *);
void skiplist_free(struct skiplist *);

/* skiplsit iterator */
void skiplist_iter_init(struct skiplist_iter *, struct skiplist *);
int skiplist_iter_valid(struct skiplist_iter *);
void skiplist_iter_next(struct skiplist_iter *);
void skiplist_iter_prev(struct skiplist_iter *);
void skiplist_iter_seek(struct skiplist_iter *, void *);
void skiplist_iter_seektofirst(struct skiplist_iter *);
void skiplist_iter_seektolast(struct skiplist_iter *);

#endif /* _nessDB_SKIPLIST_H_ */
