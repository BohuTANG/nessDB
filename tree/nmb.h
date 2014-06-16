/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_NMB_H_
#define nessDB_NMB_H_

#include "internal.h"
#include "mempool.h"
#include "mb.h"

struct nmb {
	uint32_t count;
	uint32_t memory_used;
	struct pma *pma;
	struct mempool *mpool;
};

struct nmb *nmb_new();
uint32_t nmb_memsize(struct nmb*);
uint32_t nmb_count(struct nmb*);
void nmb_put(struct nmb*, MSN, msgtype_t, struct msg*, struct msg*, struct txnid_pair*);
void nmb_free(struct nmb*);

void nmb_iter_init(struct mb_iter*, struct nmb*);
int nmb_iter_valid(struct mb_iter*);
void nmb_iter_next(struct mb_iter*);
void nmb_iter_prev(struct mb_iter*);
void nmb_iter_seek(struct mb_iter*, struct msg*);
void nmb_iter_seektofirst(struct mb_iter*);
void nmb_iter_seektolast(struct mb_iter*);

#endif /* nessDB_NMB_H_ */
