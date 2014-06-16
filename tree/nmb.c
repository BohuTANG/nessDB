/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "debug.h"
#include "nmb.h"

struct nmb *nmb_new() {
	struct nmb *nmb = xmalloc(sizeof(*nmb));

	nmb->mpool = mempool_new();
	nmb->pma = pma_new(64);
	nmb->count = 0;
	nmb->memory_used = 0;

	return nmb;
}

void nmb_put(struct nmb *nmb,
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

	base = mempool_alloc_aligned(nmb->mpool, size);
	msgentry_pack(base, msn, type, key, val, xidpair);
	pma_insert(nmb->pma, (void*)base, size, msgentry_key_compare);
	nmb->count++;
	nmb->memory_used += size;
}

uint32_t nmb_memsize(struct nmb *nmb)
{
	return nmb->memory_used;
}

uint32_t nmb_count(struct nmb *nmb)
{
	return nmb->count;
}

void nmb_free(struct nmb *nmb)
{
	if (!nmb)
		return;

	pma_free(nmb->pma);
	mempool_free(nmb->mpool);
}

void nmb_iter_init(struct mb_iter *iter, struct nmb *nmb)
{
	iter->valid = 0;
	iter->pma = nmb->pma;

	iter->chain_idx = 0;
	iter->array_idx = 0;
}

/* valid */
int nmb_iter_valid(struct mb_iter *iter)
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

void nmb_iter_next(struct mb_iter *iter)
{
	char *base;
	struct pma *pma = iter->pma;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	nassert(nmb_iter_valid(iter) == 1);
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

void nmb_iter_prev(struct mb_iter *iter)
{
	char *base;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;
	struct pma *pma = iter->pma;

	nassert(nmb_iter_valid(iter) == 1);
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

void nmb_iter_seek(struct mb_iter *iter, struct msg *k)
{
	char *base = NULL;
	int size = MSGENTRY_SIZE;
	struct msgentry *mentry;

	if (!k) return;

	size += k->size;
	mentry = (struct msgentry*)xcalloc(1, size);

	mentry->keylen = k->size;
	mentry->vallen = 0U;
	memcpy((char*)mentry + MSGENTRY_SIZE, k->data, k->size);
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

void nmb_iter_seektofirst(struct mb_iter *iter)
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

void nmb_iter_seektolast(struct mb_iter *iter)
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
