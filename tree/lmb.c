/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "debug.h"
#include "mb.h"
#include "lmb.h"

struct lmb *lmb_new() {
	struct lmb *lmb = xmalloc(sizeof(*lmb));

	lmb->pma = pma_new(64);

	return lmb;
}

void lmb_put(struct lmb *lmb,
             MSN msn,
             msgtype_t type,
             struct msg *key,
             struct msg *val,
             struct txnid_pair *xidpair)
{
	char *base;
	uint32_t size = 0U;

	size += (MSGENTRY_SIZE + key->size);
	if (type != MSG_DELETE)
		size += val->size;

	base = xcalloc(1, size);
	msgentry_pack(base, msn, type, key, val, xidpair);
	pma_insert(lmb->pma, (void*)base, size, msgentry_key_compare);
}

uint32_t lmb_memsize(struct lmb *lmb)
{
	return lmb->pma->memory_used;
}

uint32_t lmb_count(struct lmb *lmb)
{
	return lmb->pma->count;
}

void lmb_free(struct lmb *lmb)
{
	if (!lmb)
		return;

	int cidx;
	int aidx;

	for (cidx = 0; cidx < lmb->pma->used; cidx++) {
		struct array *a = lmb->pma->chain[cidx];

		for (aidx = 0; aidx < a->used; aidx++) {
			void *v = a->elems[aidx];
			xfree(v);
		}
	}

	pma_free(lmb->pma);
	xfree(lmb);
}

void lmb_iter_init(struct mb_iter *iter, struct lmb *lmb)
{
	iter->valid = 0;
	iter->pma = lmb->pma;

	iter->chain_idx = 0;
	iter->array_idx = 0;
}

/* valid */
int lmb_iter_valid(struct mb_iter *iter)
{
	int valid = 0;
	struct pma *p = iter->pma;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	if ((chain_idx < p->used)
	    && (array_idx < p->chain[chain_idx]->used))
		valid = 1;

	return valid;
}

void lmb_iter_next(struct mb_iter *iter)
{
	char *base;
	struct pma *pma = iter->pma;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	nassert(lmb_iter_valid(iter) == 1);
	base = pma->chain[chain_idx]->elems[array_idx];
	msgentry_unpack(base,
	                &iter->msn,
	                &iter->type,
	                &iter->key,
	                &iter->val,
	                &iter->xidpair);
	if (array_idx == (pma->chain[chain_idx]->used - 1)) {
		iter->chain_idx++;
		iter->array_idx = 0;
	} else {
		iter->array_idx++;
	}
}

void lmb_iter_prev(struct mb_iter *iter)
{
	char *base;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;
	struct pma *pma = iter->pma;

	nassert(lmb_iter_valid(iter) == 1);
	base = pma->chain[chain_idx]->elems[array_idx];
	msgentry_unpack(base,
	                &iter->msn,
	                &iter->type,
	                &iter->key,
	                &iter->val,
	                &iter->xidpair);
	if (array_idx == 0) {
		iter->chain_idx--;
		iter->array_idx = pma->chain[chain_idx]->used - 1;
	} else {
		iter->array_idx--;
	}
}

void lmb_iter_seek(struct mb_iter *iter, struct msg *k)
{
	char *base = NULL;
	int size = MSGENTRY_SIZE;
	struct msgentry *mentry;

	if (!k) return;

	size += k->size;
	mentry = (struct msgentry*)xcalloc(1, size);

	mentry->keylen = k->size;
	mentry->vallen = 0U;
	xmemcpy((char*)mentry + MSGENTRY_SIZE, k->data, k->size);
	//get base
	// pma seek
	xfree(mentry);

	msgentry_unpack(base,
	                &iter->msn,
	                &iter->type,
	                &iter->key,
	                &iter->val,
	                &iter->xidpair);
}

void lmb_iter_seektofirst(struct mb_iter *iter)
{
	char *base;
	struct pma *pma = iter->pma;

	if (pma->used > 0) {
		base = pma->chain[0]->elems[0];
		msgentry_unpack(base,
		                &iter->msn,
		                &iter->type,
		                &iter->key,
		                &iter->val,
		                &iter->xidpair);
	}
}

void lmb_iter_seektolast(struct mb_iter *iter)
{
	char *base;
	struct pma *pma = iter->pma;
	struct array *array = pma->chain[pma->used - 1];

	base = array->elems[array->used - 1];
	msgentry_unpack(base,
	                &iter->msn,
	                &iter->type,
	                &iter->key,
	                &iter->val,
	                &iter->xidpair);
}

int lmb_split(struct lmb *mb,
              struct lmb ** a,
              struct lmb ** b,
              struct msg ** split_key)
{
	int ret;
	struct lmb *mba = mb;
	struct lmb *mbb;

	struct pma *pa = mb->pma;
	struct pma *pb;

	if (pa->used < 2) {
		ret = NESS_ERR;
		goto ERR;
	}

	int a_used = pa->used / 2;
	int b_used = pa->used - a_used;
	struct array *last_array_in_a = pa->chain[a_used - 1];
	char *midbase = last_array_in_a->elems[last_array_in_a->used - 1];

	/* mbb */
	mbb = lmb_new();
	pb = mbb->pma;
	pma_resize(pb, b_used);

	/* copy chain elems to new pma */
	xmemcpy(pb->chain, pa->chain + a_used, b_used * sizeof(void*));

	/* update count */
	pa->used = a_used;
	pb->used = b_used;
	pa->count = pma_count_calc(pa);
	pb->count = pma_count_calc(pb);

	/* memory size */
	pa->memory_used = pma_memsize_calc(pa);
	pb->memory_used = pma_memsize_calc(pb);

	/* split key */
	struct mb_iter iter;

	lmb_iter_init(&iter, mb);
	msgentry_unpack(midbase,
	                &iter.msn,
	                &iter.type,
	                &iter.key,
	                &iter.val,
	                &iter.xidpair);

	*split_key = msgdup(&iter.key);

	*a = mba;
	*b = mbb;

	ret = NESS_OK;
ERR:
	return ret;
}
