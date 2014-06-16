/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LMB_H_
#define nessDB_LMB_H_

#include "internal.h"
#include "mb.h"

struct lmb {
	struct pma *pma;
};

struct lmb *lmb_new();
uint32_t lmb_memsize(struct lmb*);
uint32_t lmb_count(struct lmb*);
void lmb_put(struct lmb*, MSN, msgtype_t, struct msg*, struct msg*, struct txnid_pair*);
void lmb_free(struct lmb*);

void lmb_iter_init(struct mb_iter*, struct lmb*);
int lmb_iter_valid(struct mb_iter*);
void lmb_iter_next(struct mb_iter*);
void lmb_iter_prev(struct mb_iter*);
void lmb_iter_seek(struct mb_iter*, struct msg*);
void lmb_iter_seektofirst(struct mb_iter*);
void lmb_iter_seektolast(struct mb_iter*);

int lmb_split(struct lmb *mb,
              struct lmb ** a,
              struct lmb ** b,
              struct msg ** split_key);

#endif /* nessDB_LMB_H_ */
