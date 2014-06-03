/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "compare-func.h"
#include "msgbuf.h"

void _msgentry_pack(char *data,
                    MSN msn,
                    msgtype_t type,
                    struct msg *key,
                    struct msg *val,
                    struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t vlen = 0U;
	uint32_t klen = key->size;
	struct msgentry *entry = (struct msgentry*)data;

	if (type != MSG_DELETE)
		vlen = val->size;

	entry->msn = msn;
	entry->type = type;
	entry->xidpair = *xidpair;
	entry->keylen = klen;
	entry->vallen = vlen;
	pos += MSGENTRY_SIZE;

	memcpy(data + pos, key->data, key->size);
	pos += key->size;

	if (vlen > 0) {
		memcpy(data + pos, val->data, val->size);
		pos += val->size;
	}
}

void _msgentry_unpack(char *data,
                      MSN *msn,
                      msgtype_t *type,
                      struct msg *key,
                      struct msg *val,
                      struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t klen;
	uint32_t vlen;
	struct msgentry *entry;

	entry = (struct msgentry*)data;
	*msn = entry->msn;
	*type = entry->type;
	*xidpair = entry->xidpair;
	klen = entry->keylen;
	vlen = entry->vallen;
	pos += MSGENTRY_SIZE;

	key->size = klen;
	key->data = (data + pos);
	pos += klen;

	if (*type != MSG_DELETE) {
		val->size = vlen;
		val->data = (data + pos);
		pos += vlen;
	}
}

/* msgentry size calc */
uint32_t _msgentry_size(struct msg *k, struct msg *v)
{
	uint32_t size = MSGENTRY_SIZE + k->size;

	if (v)
		size += v->size;

	return size;
}

struct msgbuf *msgbuf_new() {
	struct msgbuf *mb;

	mb = xcalloc(1, sizeof(*mb));
	mb->pma = pma_new(128);

	return mb;
}

void msgbuf_put(struct msgbuf *mb,
                MSN msn,
                msgtype_t type,
                struct msg *key,
                struct msg *val,
                struct txnid_pair *xidpair)
{
	uint32_t sizes = _msgentry_size(key, val);
	char *base = xmalloc(sizes);

	_msgentry_pack(base, msn, type, key, val, xidpair);
	pma_insert(mb->pma, (void*)base, sizes, msgbuf_key_compare);
}


/*
 * it's all alloced size, not used size
 */
uint32_t msgbuf_memsize(struct msgbuf *mb)
{
	return mb->pma->memory_used;
}

uint32_t msgbuf_count(struct msgbuf *mb)
{
	return mb->pma->count;
}

void msgbuf_free(struct msgbuf *mb)
{
	if (!mb) return;

	int cidx;
	int aidx;

	for (cidx = 0; cidx < mb->pma->used; cidx++) {
		struct array *a = mb->pma->chain[cidx];

		for (aidx = 0; aidx < a->used; aidx++) {
			void *v = a->elems[aidx];
			xfree(v);
		}
	}

	pma_free(mb->pma);
	xfree(mb);
}

/*******************************************************
 * msgbuf iterator (thread-safe)
*******************************************************/

void _iter_unpack(char *base, struct msgbuf_iter *iter)
{
	if (base) {
		_msgentry_unpack(base,
		                 &iter->msn,
		                 &iter->type,
		                 &iter->key,
		                 &iter->val,
		                 &iter->xidpair);
		iter->valid = 1;
	} else {
		iter->valid = 0;
	}
}

/* init */
void msgbuf_iter_init(struct msgbuf_iter *iter, struct msgbuf *mb)
{
	iter->valid = 0;
	iter->mb = mb;
	iter->pma = mb->pma;

	iter->chain_idx = 0;
	iter->array_idx = 0;
}

int msgbuf_split(struct msgbuf *mb,
                 struct msgbuf** a,
                 struct msgbuf** b,
                 struct msg ** split_key)
{
	int ret;
	struct msgbuf *mba = mb;
	struct msgbuf *mbb;

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
	mbb = msgbuf_new();
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
	struct msgbuf_iter iter;

	msgbuf_iter_init(&iter, mb);
	_iter_unpack(midbase, &iter);

	*split_key = msgdup(&iter.key);

	*a = mba;
	*b = mbb;

	ret = NESS_OK;
ERR:
	return ret;
}

/* valid */
int msgbuf_iter_valid(struct msgbuf_iter *iter)
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

/*
 * msgbuf iterator next is special
 * we just arrive at the top version of key
 */
void msgbuf_iter_next(struct msgbuf_iter *iter)
{
	char *base;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;
	struct pma *pma = iter->pma;

	nassert(msgbuf_iter_valid(iter) == 1);

	base = pma->chain[chain_idx]->elems[array_idx];
	_iter_unpack(base, iter);

	if (array_idx == (pma->chain[chain_idx]->used - 1)) {
		iter->chain_idx++;
		iter->array_idx = 0;
	} else {
		iter->array_idx++;
	}
}

/* prev is normal */
void msgbuf_iter_prev(struct msgbuf_iter *iter)
{
	char *base;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;
	struct pma *pma = iter->pma;

	nassert(msgbuf_iter_valid(iter) == 1);

	base = pma->chain[chain_idx]->elems[array_idx];
	_iter_unpack(base, iter);

	if (array_idx == 0) {
		iter->chain_idx--;
		iter->array_idx = pma->chain[chain_idx]->used - 1;
	} else {
		iter->array_idx--;
	}
}

/*
 * seek
 * when we do msgbuf_iter_seek('key1')
 * the postion in msgbuf is >= postion('key1')
 */
void msgbuf_iter_seek(struct msgbuf_iter *iter, struct msg *k)
{
	int size;
	char *base = NULL;
	struct msgentry *mentry;

	if (!k) return;
	size = _msgentry_size(k, NULL);
	mentry = (struct msgentry*)xcalloc(1, size);

	mentry->keylen = k->size;
	mentry->vallen = 0U;
	memcpy((char*)mentry + MSGENTRY_SIZE, k->data, k->size);

	// pma seek
	xfree(mentry);

	_iter_unpack(base, iter);
}

/* seek to first */
void msgbuf_iter_seektofirst(struct msgbuf_iter *iter)
{
	char *base;
	struct pma *pma = iter->pma;

	if (pma->used > 0) {
		base = pma->chain[0]->elems[0];
		_iter_unpack(base, iter);
	}
}

/* seek to last */
void msgbuf_iter_seektolast(struct msgbuf_iter *iter)
{
	char *base;
	struct pma *pma = iter->pma;
	struct array *array = pma->chain[pma->used - 1];

	base = array->elems[array->used - 1];
	_iter_unpack(base, iter);
}
