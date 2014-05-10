/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MSGBUF_H_
#define nessDB_MSGBUF_H_

#include "internal.h"
#include "mempool.h"
#include "skiplist.h"
#include "msg.h"

/**
 * @file msgbuf.h
 * @brief msgbuf definitions
 *
 * msgbuf is a sorted-structure and multi-version for nodes
 * the key/val layout as follows:
 * +--+----+----+---
 * |k1|msn1|val1|   \
 * +--+----+----+    \
 * |k1|msn2|val2|     k1 multi-version
 * +--+----+----+    /
 * |k1|msn3|val3|   /
 * +--+----+----+---
 * |k2|msn4|val1|
 * +--+----+----+
 * |k2|msn5|val2|
 * +--+----+----+
 *
 */

struct msgbuf_iter {
	int valid;
	MSN msn;
	msgtype_t type;
	struct msg key;
	struct msg val;
	struct txnid_pair xidpair;
	struct msgbuf *mb;
	struct skiplist_iter list_iter;

	/* internal info */
	int idx;
};

struct msgbuf {
	uint32_t count;
	struct mempool *mpool;
	struct skiplist *list;
};

struct msgbuf *msgbuf_new(void);

void msgbuf_put(struct msgbuf *,
                MSN,
                msgtype_t,
                struct msg *,
                struct msg *,
                struct txnid_pair *);

uint32_t msgbuf_memsize(struct msgbuf *);
uint32_t msgbuf_count(struct msgbuf *);
void msgbuf_free(struct msgbuf *);

/* msgbuf iterator */
void msgbuf_iter_init(struct msgbuf_iter *, struct msgbuf *);
int msgbuf_iter_valid(struct msgbuf_iter *);
int msgbuf_iter_valid_lessorequal(struct msgbuf_iter *, struct msg *);
void msgbuf_iter_next(struct msgbuf_iter *);
int msgbuf_internal_iter_next(struct msgbuf_iter *);
int msgbuf_internal_iter_reverse(struct msgbuf_iter *);
void msgbuf_iter_prev(struct msgbuf_iter *);
void msgbuf_iter_seek(struct msgbuf_iter *, struct msg *);
void msgbuf_iter_seektofirst(struct msgbuf_iter *);
void msgbuf_iter_seektolast(struct msgbuf_iter *);

#endif /* _nessDB_MSGBUF_H_ */
