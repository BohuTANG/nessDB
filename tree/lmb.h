/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_LMB_H_
#define nessDB_LMB_H_

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
	uint16_t xrs_size;
	uint32_t num_pxrs;		/* how many of uxrs[] are provisional */
	uint32_t num_cxrs;		/* how many of uxrs[] are committed */
	struct xr *xrs;
};

struct lmb_values {
	struct msg key;
	struct leafentry *le;
};

struct lmb {
	uint32_t count;
	struct pma *pma;
	struct mempool *mpool;
	struct tree_options *opts;
};

struct lmb *lmb_new(struct tree_options *opts);
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

int lmb_find_zero(struct lmb *,
                  struct msg *,
                  struct leafentry **,
                  struct pma_coord *);

int lmb_find_plus(struct lmb *,
                  struct msg *,
                  struct leafentry **,
                  struct pma_coord *);

int lmb_find_minus(struct lmb *,
                   struct msg *,
                   struct leafentry **,
                   struct pma_coord *);

void lmb_get_values(struct mb_iter *, struct lmb_values *);
void lmb_to_msgpack(void *packer, void *msgbuf);
void msgpack_to_lmb(void *packer, void *msgbuf);

#endif /* nessDB_LMB_H_ */
