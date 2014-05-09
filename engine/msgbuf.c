/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "compare-func.h"
#include "msgbuf.h"

void _msg_entry_pack(char *data,
                     MSN msn,
                     msgtype_t type,
                     struct msg *key,
                     struct msg *val,
                     struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t vlen = 0U;
	uint32_t klen = key->size;
	struct msg_entry *entry = (struct msg_entry*)data;

	if (type != MSG_DELETE)
		vlen = val->size;

	entry->msn = msn;
	entry->type = type;
	entry->xidpair = *xidpair;
	entry->keylen = klen;
	entry->vallen = vlen;
	pos += MSG_ENTRY_SIZE;

	memcpy(data + pos, key->data, key->size);
	pos += key->size;

	if (type != MSG_DELETE) {
		memcpy(data + pos, val->data, val->size);
		pos += val->size;
	}
}

void _msg_entry_unpack(char *data,
                       MSN *msn,
                       msgtype_t *type,
                       struct msg *key,
                       struct msg *val,
                       struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t klen;
	uint32_t vlen;
	struct msg_entry *entry;

	entry = (struct msg_entry*)data;
	*msn = entry->msn;
	*type = entry->type;
	*xidpair = entry->xidpair;
	klen = entry->keylen;
	vlen = entry->vallen;
	pos += MSG_ENTRY_SIZE;

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
}

struct msgbuf *msgbuf_new() {
	struct msgbuf *mb;

	mb = xcalloc(1, sizeof(*mb));
	mb->mpool = mempool_new();
	mb->list = skiplist_new(msgbuf_key_compare);
	return mb;
}

void msgbuf_put(struct msgbuf *mb,
                MSN msn,
                msgtype_t type,
                struct msg *key,
                struct msg *val,
                struct txnid_pair *xidpair)
{
	char *base;
	uint32_t sizes = 0U;

	sizes += (MSG_ENTRY_SIZE + key->size);
	if (type != MSG_DELETE)
		sizes += val->size;
	sizes += sizeof(*xidpair);
	base = mempool_alloc_aligned(mb->mpool, sizes);
	_msg_entry_pack(base, msn, type, key, val, xidpair);
	skiplist_put(mb->list, base);
	mb->count++;
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
	return mb->count;
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

void _iter_unpack(const char *base, struct msgbuf_iter *iter)
{
	if (base) {
		_msg_entry_unpack((char*)base,
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
	iter->idx = 0;
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
	void *base = NULL;

	iter->idx = 0;
	skiplist_iter_next(&iter->list_iter);
	if (skiplist_iter_valid(&iter->list_iter)) {
		base = iter->list_iter.node->keys[0];
	}

	_iter_unpack(base, iter);
}

/*
 * same key array iterator
 */
int msgbuf_internal_iter_next(struct msgbuf_iter *iter)
{
	void *base = NULL;

	if (iter->idx < iter->list_iter.node->used) {
		base = iter->list_iter.node->keys[iter->idx++];
		_iter_unpack(base, iter);
		return 1;
	} else {
		return 0;
	}
}

int msgbuf_internal_iter_reverse(struct msgbuf_iter *iter)
{
	void *base = NULL;

	iter->idx = iter->list_iter.node->used - 1;
	if (iter->idx > -1) {
		base = iter->list_iter.node->keys[iter->idx--];
		_iter_unpack(base, iter);
		return 1;
	} else {
		return 0;
	}
}

/*
 * the last one of the same key array
 */
int msgbuf_internal_iter_last(struct msgbuf_iter *iter)
{
	void *base = NULL;

	iter->valid = 0;
	iter->idx = iter->list_iter.node->used - 1;
	if (iter->idx > -1) {
		base = iter->list_iter.node->keys[iter->idx];
		_iter_unpack(base, iter);

		return 1;
	} else {
		return 0;
	}
}

/* prev is normal */
void msgbuf_iter_prev(struct msgbuf_iter *iter)
{
	void *base = NULL;

	iter->idx = 0;
	skiplist_iter_prev(&iter->list_iter);
	if (iter->list_iter.node) {
		base = iter->list_iter.node->keys[0];
	}

	_iter_unpack(base, iter);
}

/*
 * seek
 * when we do msgbuf_iter_seek('key1')
 * the postion in msgbuf is >= postion('key1')
 */
void msgbuf_iter_seek(struct msgbuf_iter *iter, struct msg *k)
{
	int size;
	char *data;
	void *base = NULL;
	struct msg_entry *entry;

	if (!k) return;
	size = (MSG_ENTRY_SIZE + k->size);
	data = xcalloc(1, size);
	entry = (struct msg_entry*)(data);
	entry->keylen = k->size;
	entry->vallen = 0U;
	memcpy(data + MSG_ENTRY_SIZE, k->data, k->size);
	skiplist_iter_seek(&iter->list_iter, data);
	xfree(data);

	iter->idx = 0;
	if (iter->list_iter.node)
		base = iter->list_iter.node->keys[0];

	_iter_unpack(base, iter);
}

/* seek to first */
void msgbuf_iter_seektofirst(struct msgbuf_iter *iter)
{
	void *base = NULL;

	iter->idx = 0;
	skiplist_iter_seektofirst(&iter->list_iter);
	if (iter->list_iter.node)
		base = iter->list_iter.node->keys[0];

	_iter_unpack(base, iter);
}

/* seek to last */
void msgbuf_iter_seektolast(struct msgbuf_iter *iter)
{
	void *base = NULL;

	iter->idx = 0;
	skiplist_iter_seektolast(&iter->list_iter);
	if (iter->list_iter.node)
		base = iter->list_iter.node->keys[0];

	_iter_unpack(base, iter);
}
