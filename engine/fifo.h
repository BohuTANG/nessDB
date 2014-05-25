/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_FIFO_H_
#define nessDB_FIFO_H_

#include "internal.h"
#include "mempool.h"
#include "msg.h"

struct fifo_iter {
	int valid;
	MSN msn;
	msgtype_t type;
	struct msg key;
	struct msg val;
	struct txnid_pair xidpair;
	struct fifo *fifo;
	uint32_t used;
	int blk_curr;
};

struct  fifo {
	uint32_t count;
	struct mempool *mpool;
};

struct fifo *fifo_new(void);
uint32_t fifo_memsize(struct fifo *);
uint32_t fifo_count(struct fifo *);
void fifo_append(struct fifo*, MSN, msgtype_t, struct msg*, struct msg*, struct txnid_pair*);
void fifo_free(struct fifo*);

void fifo_iter_init(struct fifo_iter *, struct fifo *);
int fifo_iter_next(struct fifo_iter *);

#endif /* nessDB_FIFO_H_ */
