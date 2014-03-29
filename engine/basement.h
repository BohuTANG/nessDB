/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_BASEMENT_H_
#define nessDB_BASEMENT_H_

#include "internal.h"
#include "mempool.h"
#include "skiplist.h"
#include "msg.h"

/**
 * @file basement.h
 * @brief basement definitions
 *
 * basement is a sorted-structure for nodes
 *
 */

struct basement_iter {
	int valid;
	MSN msn;
	msgtype_t type;
	struct msg key;
	struct msg val;
	struct txnid_pair xidpair;
	struct basement *bsm;
	struct skiplist_iter list_iter;
};

struct basement {
	uint32_t count;
	struct mempool *mpool;
	struct skiplist *list;
};

struct basement *basement_new();

void basement_put(struct basement *bsm,
                  MSN msn,
                  msgtype_t type,
                  struct msg *key,
                  struct msg *val,
                  struct txnid_pair *idpair);

uint32_t basement_memsize(struct basement *);
uint32_t basement_count(struct basement *);
void basement_free(struct basement *);

/* basement iterator */
void basement_iter_init(struct basement_iter *, struct basement *);
int basement_iter_valid(struct basement_iter *);
void basement_iter_next(struct basement_iter *);
void basement_iter_prev(struct basement_iter *);
void basement_iter_seek(struct basement_iter *, struct msg *);
void basement_iter_seektofirst(struct basement_iter *);
void basement_iter_seektolast(struct basement_iter *);

#endif /* _nessDB_BASEMENT_H_ */
