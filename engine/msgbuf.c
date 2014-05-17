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

/* resize arrays size to double */
struct msgarray *_msgarray_new(struct msgbuf *mb) {
	struct msgarray *marray;

	marray = (struct msgarray*)mempool_alloc_aligned(mb->mpool, sizeof(struct msgarray));
	memset(marray, 0, sizeof(*marray));
	marray->size = MSGENTRY_ARRAY_SLOTS;
	marray->arrays = (void*)mempool_alloc_aligned(mb->mpool, marray->size * sizeof(void*));

	return marray;
}

struct msgbuf *msgbuf_new() {
	struct msgbuf *mb;

	mb = xcalloc(1, sizeof(*mb));
	mb->mpool = mempool_new();
	mb->list = skiplist_new(msgbuf_key_compare);
	mb->marray = (struct msgarray*)mempool_alloc_aligned(mb->mpool, sizeof(struct msgarray));
	memset(mb->marray, 0, sizeof(*mb->marray));
	mb->marray->size = MSGENTRY_ARRAY_SLOTS;
	mb->marray->arrays = (void*)mempool_alloc_aligned(mb->mpool, mb->marray->size * sizeof(void*));

	return mb;
}

void _msgarray_resize(struct msgbuf *mb, struct msgarray *marray)
{
	int newsize;
	void *newarrays;

	newsize = marray->size * 2;
	newarrays = mempool_alloc_aligned(mb->mpool, newsize * sizeof(void*));
	xmemcpy(newarrays, marray->arrays, marray->used * sizeof(void*));

	marray->size = newsize;
	marray->arrays = newarrays;
}

void msgbuf_put(struct msgbuf *mb,
                MSN msn,
                msgtype_t type,
                struct msg *key,
                struct msg *val,
                struct txnid_pair *xidpair)
{
	char *base;
	int newroom;
	uint32_t sizes;
	struct msgarray *marray;
	struct skipnode *node;

	sizes = _msgentry_size(key, val);
	base = mempool_alloc_aligned(mb->mpool, sizes);
	_msgentry_pack(base, msn, type, key, val, xidpair);
	mb->marray->arrays[0] = (struct msgentry*)base;

	newroom = skiplist_makeroom(mb->list, (void*)mb->marray, &node);
	if (newroom) {
		marray = _msgarray_new(mb);
		marray->arrays[marray->used++] = (struct msgentry*)base;
		if (xidpair->child_xid == TXNID_NONE)
			marray->num_cx++;
		else
			marray->num_px++;

		(node)->key = (void*)marray;
	} else {
		/* just update the arrays */
		marray = (struct msgarray*)(node)->key;
		if (xidpair->child_xid != TXNID_NONE) {
			if (marray->used >= marray->size)
				_msgarray_resize(mb, marray);
			marray->arrays[marray->used++] = (struct msgentry*)base;
			marray->num_px++;
		} else {
			marray->arrays[0] = (struct msgentry*)base;
			marray->used = 1;
			marray->num_cx = 1;
			marray->num_px = 0;
		}
	}
}

/*
 * it's all alloced size, not used size
 */
uint32_t msgbuf_memsize(struct msgbuf *mb)
{
	return mb->mpool->memory_used;
}

uint32_t msgbuf_count(struct msgbuf *mb)
{
	return mb->list->count;
}

void msgbuf_free(struct msgbuf *mb)
{
	if (!mb) return;

	mempool_free(mb->mpool);
	skiplist_free(mb->list);
	xfree(mb);
}

/*******************************************************
 * msgbuf iterator (thread-safe)
*******************************************************/

void _iter_unpack(struct msgarray *marray, struct msgbuf_iter *iter)
{
	iter->mvcc = 0;
	if (marray) {
		char *base = (char*)marray->arrays[iter->idx];

		_msgentry_unpack(base,
		                 &iter->msn,
		                 &iter->type,
		                 &iter->key,
		                 &iter->val,
		                 &iter->xidpair);
		iter->valid = 1;
		iter->mvcc = marray->used > 1;
	} else {
		iter->valid = 0;
	}
	iter->marray = marray;
}

/* init */
void msgbuf_iter_init(struct msgbuf_iter *iter, struct msgbuf *mb)
{
	iter->valid = 0;
	iter->idx = 0;
	iter->mvcc = 0;
	iter->mb = mb;
	skiplist_iter_init(&iter->list_iter, mb->list);
}

/* valid */
int msgbuf_iter_valid(struct msgbuf_iter *msgbuf_iter)
{
	return skiplist_iter_valid(&msgbuf_iter->list_iter);
}

/* valid and less or equal */
int msgbuf_iter_valid_lessorequal(struct msgbuf_iter *iter, struct msg *key)
{
	return (skiplist_iter_valid(&iter->list_iter) &&
	        (msg_key_compare(&iter->key, key) <= 0));
}

/*
 * msgbuf iterator next is special
 * we just arrive at the top version of key
 */
void msgbuf_iter_next(struct msgbuf_iter *iter)
{
	struct msgarray *marray = NULL;

	iter->idx = 0;
	skiplist_iter_next(&iter->list_iter);
	if (skiplist_iter_valid(&iter->list_iter)) {
		marray = (struct msgarray*)iter->list_iter.node->key;
	}

	_iter_unpack(marray, iter);
}

/*
 * same key array iterator
 */
int msgbuf_internal_iter_next(struct msgbuf_iter *iter)
{
	struct msgarray *marray = iter->marray;

	if (iter->idx < marray->used) {
		_iter_unpack(marray, iter);
		iter->idx++;

		return 1;
	} else {
		return 0;
	}
}

int msgbuf_internal_iter_reverse(struct msgbuf_iter *iter)
{
	struct msgarray *marray = iter->marray;

	iter->idx = marray->used - 1;
	if (iter->idx > -1) {
		_iter_unpack(marray, iter);
		return 1;
	} else {
		return 0;
	}
}

/* prev is normal */
void msgbuf_iter_prev(struct msgbuf_iter *iter)
{
	struct msgarray *marray = NULL;

	iter->idx = 0;
	skiplist_iter_prev(&iter->list_iter);
	if (iter->list_iter.node) {
		marray = iter->list_iter.node->key;
	}

	_iter_unpack(marray, iter);
}

/*
 * seek
 * when we do msgbuf_iter_seek('key1')
 * the postion in msgbuf is >= postion('key1')
 */
void msgbuf_iter_seek(struct msgbuf_iter *iter, struct msg *k)
{
	int size;
	struct msgentry *mentry;
	struct msgarray array;

	if (!k) return;
	size = _msgentry_size(k, NULL);
	mentry = (struct msgentry*)xcalloc(1, size);

	mentry->keylen = k->size;
	mentry->vallen = 0U;
	memcpy((char*)mentry + MSGENTRY_SIZE, k->data, k->size);

	array.arrays = xcalloc(1, sizeof(void*));
	array.arrays[0] = mentry;

	skiplist_iter_seek(&iter->list_iter, (void*)&array);
	xfree(mentry);
	xfree(array.arrays);

	struct msgarray *marray = NULL;

	iter->idx = 0;
	if (skiplist_iter_valid(&iter->list_iter))
		marray = iter->list_iter.node->key;
	_iter_unpack(marray, iter);
}

/* seek to first */
void msgbuf_iter_seektofirst(struct msgbuf_iter *iter)
{
	struct msgarray *marray = NULL;

	iter->idx = 0;
	skiplist_iter_seektofirst(&iter->list_iter);
	if (skiplist_iter_valid(&iter->list_iter))
		marray = iter->list_iter.node->key;

	_iter_unpack(marray, iter);
}

/* seek to last */
void msgbuf_iter_seektolast(struct msgbuf_iter *iter)
{
	struct msgarray *marray = NULL;

	iter->idx = 0;
	skiplist_iter_seektolast(&iter->list_iter);
	if (skiplist_iter_valid(&iter->list_iter))
		marray = iter->list_iter.node->key;

	_iter_unpack(marray, iter);
}
