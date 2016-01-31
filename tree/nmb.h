/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_NMB_H_
#define nessDB_NMB_H_

/**
 *
 * @file nmb
 * @brief inner node message buffer
 *
 */

#define NMB_ENTRY_SIZE (sizeof(struct nmb_entry))
struct nmb_entry {
	MSN msn;
	uint8_t type;
	struct txnid_pair xidpair;
	uint32_t keylen;
	uint32_t vallen;
} __attribute__((__packed__));

struct nmb_values {
	MSN msn;
	msgtype_t type;
	struct msg key;
	struct msg val;
	struct txnid_pair xidpair;
};

struct nmb {
	uint32_t count;
	struct pma *pma;
	struct mempool *mpool;
	struct tree_options *opts;
	ness_mutex_t mtx;
	ness_rwlock_t rwlock;
};

struct nmb *nmb_new(struct tree_options *opts);
void nmb_free(struct nmb*);

uint32_t nmb_memsize(struct nmb*);
uint32_t nmb_count(struct nmb*);

void nmb_put(struct nmb*,
             MSN,
             msgtype_t,
             struct msg*,
             struct msg*,
             struct txnid_pair*);

void nmb_get_values(struct mb_iter *, struct nmb_values *);
void nmb_to_msgpack(void *packer, void *msgbuf);
void msgpack_to_nmb(void *packer, void *msgbuf);

int nmb_get_left_coord(struct nmb *, struct msg *, struct pma_coord *);
int nmb_get_right_coord(struct nmb *, struct msg *, struct pma_coord *);

#endif /* nessDB_NMB_H_ */
