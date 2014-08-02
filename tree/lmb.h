/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LMB_H_
#define nessDB_LMB_H_

#include "internal.h"
#include "msgpack.h"
#include "mempool.h"
#include "mb.h"

/* transaction record */
#define XR_SIZE (sizeof(struct xr))
struct xr {
	TXNID xid;
	msgtype_t type;
	uint32_t vallen;
	void *valp;
};

#define LEAFENTRY_SIZE (sizeof(struct leafentry))
struct leafentry {
	uint32_t keylen;
	void *keyp;
	uint32_t num_pxrs;		/* how many of uxrs[] are provisional */
	uint32_t num_cxrs;		/* how many of uxrs[] are committed */
	struct xr xrs_static[32];
};

struct lmb_values {
	struct msg key;
	struct leafentry *le;
};

struct lmb {
	struct pma *pma;
	struct mempool *mpool;
};

struct lmb *lmb_new();
void lmb_free(struct lmb*);

uint32_t lmb_memsize(struct lmb*);
uint32_t lmb_count(struct lmb*);

void lmb_put(struct lmb*,
             MSN, msgtype_t,
             struct msg*,
             struct msg*,
             struct txnid_pair*);


void lmb_split(struct lmb *,
               struct lmb **,
               struct lmb **,
               struct msg **);

void lmb_get_values(struct mb_iter *, struct lmb_values *);
void lmb_to_msgpack(struct lmb *, struct msgpack *);
void msgpack_to_lmb(struct msgpack *, struct lmb *);

#endif /* nessDB_LMB_H_ */
