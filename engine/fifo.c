/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "fifo.h"
#include "debug.h"

int _fifo_entry_pack(char *data,
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

	if (type != MSG_DELETE) {
		memcpy(data + pos, val->data, val->size);
		pos += val->size;
	}

	return pos;
}

int _fifo_entry_unpack(char *data,
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
	} else {
		memset(val, 0, sizeof(*val));
	}

	return pos;
}

void _fifo_iter_unpack(const char *base, struct fifo_iter *iter)
{
	if (base) {
		iter->used += _fifo_entry_unpack((char*)base,
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

struct fifo *fifo_new(void) {
	struct fifo *fifo;

	fifo = xcalloc(1, sizeof(*fifo));
	fifo->mpool = mempool_new();

	return fifo;
}

uint32_t fifo_memsize(struct fifo *fifo)
{
	return fifo->mpool->memory_used;
}

uint32_t fifo_count(struct fifo *fifo)
{
	return fifo->count;
}

void fifo_free(struct fifo *fifo)
{
	if (!fifo) return;

	mempool_free(fifo->mpool);
	xfree(fifo);
}

void fifo_append(struct fifo *fifo,
                 MSN msn,
                 msgtype_t type,
                 struct msg *key,
                 struct msg *val,
                 struct txnid_pair *xidpair)
{
	char *base;
	uint32_t sizes = 0U;
	uint32_t ret __attribute__((__unused__));

	sizes += (MSGENTRY_SIZE + key->size);
	if (type != MSG_DELETE)
		sizes += val->size;
	base = mempool_alloc(fifo->mpool, sizes);
	ret = _fifo_entry_pack(base, msn, type, key, val, xidpair);
	nassert(ret == sizes);
	fifo->count++;
}

/* init */
void fifo_iter_init(struct fifo_iter *iter, struct fifo *fifo)
{
	iter->valid = 0;
	iter->used = 0;
	iter->blk_curr = 0;
	iter->fifo = fifo;
}

int fifo_iter_next(struct fifo_iter *iter)
{
	int ret = 0;
	struct mempool *mpool;

	mpool = iter->fifo->mpool;
	if (iter->blk_curr < mpool->blk_used) {
		struct memblk *blk = mpool->blks[iter->blk_curr];
		uint32_t used = (blk->size - blk->remaining);

		if (iter->used < used) {
			_fifo_iter_unpack(blk->base + iter->used, iter);
			if (iter->used == used) {
				iter->used = 0;
				iter->blk_curr++;
			}
			ret = 1;
		}
	}

	return ret;
}
