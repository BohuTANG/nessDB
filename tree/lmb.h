/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LMB_H_
#define nessDB_LMB_H_

#include "internal.h"
#include "mempool.h"
#include "mb.h"

/* unpacked transaction record */
struct uxr {
	msgtype_t type;
	uint32_t vallen;
	void *valp;
	TXNID xid;
};

struct leafentry_mvcc {
	uint32_t num_puxrs;		/* how many of uxrs[] are provisional */
	uint32_t num_cuxrs;		/* how many of uxrs[] are committed */
	struct uxr uxrs_static[32];
};

/* unpacked leaf entry, value of the pma */
struct leafentry {
	struct leafentry_mvcc mvcc;
};

struct lmb_iter {
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

struct lmb {
	uint32_t count;
	uint32_t memory_used;
	struct pma *pma;
	struct mempool *mpool;
};

struct lmb *lmb_new();
void lmb_free(struct lmb*);
uint32_t lmb_memsize(struct lmb*);
uint32_t lmb_count(struct lmb*);
void lmb_put(struct lmb*, MSN, msgtype_t, struct msg*, struct msg*, struct txnid_pair*);

void lmb_iter_init(struct lmb_iter*, struct lmb*);
int lmb_iter_valid(struct lmb_iter*);
void lmb_iter_next(struct lmb_iter*);
void lmb_iter_prev(struct lmb_iter*);
void lmb_iter_seek(struct lmb_iter*, struct msg*);
void lmb_iter_seektofirst(struct lmb_iter*);
void lmb_iter_seektolast(struct lmb_iter*);

int lmb_split(struct lmb *mb,
              struct lmb ** a,
              struct lmb ** b,
              struct msg ** split_key);

#endif /* nessDB_LMB_H_ */
