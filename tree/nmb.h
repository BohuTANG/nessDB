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

struct nmb_iter {
	int valid;
	MSN msn;
	msgtype_t type;
	struct msg key;
	struct msg val;
	struct txnid_pair xidpair;

	struct pma *pma;
	int chain_idx;
	int array_idx;
};

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

void nmb_iter_init(struct nmb_iter*, struct nmb*);
int nmb_iter_valid(struct nmb_iter*);
void nmb_iter_next(struct nmb_iter*);
void nmb_iter_prev(struct nmb_iter*);
void nmb_iter_seek(struct nmb_iter*, struct msg*);
void nmb_iter_seektofirst(struct nmb_iter*);
void nmb_iter_seektolast(struct nmb_iter*);

#endif /* nessDB_NMB_H_ */
