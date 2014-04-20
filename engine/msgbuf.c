/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "compare-func.h"
#include "msgbuf.h"

void _encode(char *data,
             MSN msn,
             msgtype_t type,
             struct msg *key,
             struct msg *val,
             struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t vlen = 0U;
	uint32_t klen = key->size;
	struct append_entry *entry = (struct append_entry*)data;

	if (type != MSG_DELETE)
		vlen = val->size;

	entry->msn = msn;
	entry->type = type;
	entry->xidpair = *xidpair;
	entry->keylen = klen;
	entry->vallen = vlen;
	pos += get_entrylen(entry);

	memcpy(data + pos, key->data, key->size);
	pos += key->size;

	if (type != MSG_DELETE) {
		memcpy(data + pos, val->data, val->size);
		pos += val->size;
	}
}

void _decode(char *data,
             MSN *msn,
             msgtype_t *type,
             struct msg *key,
             struct msg *val,
             struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t klen;
	uint32_t vlen;
	struct append_entry *entry;

	entry = (struct append_entry*)data;
	*msn = entry->msn;
	*type = entry->type;
	*xidpair = entry->xidpair;
	klen = entry->keylen;
	vlen = entry->vallen;
	pos += get_entrylen(entry);

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
	struct msgbuf *bsm;

	bsm = xcalloc(1, sizeof(*bsm));
	bsm->mpool = mempool_new();
	bsm->list = skiplist_new(internal_key_compare);
	return bsm;
}

/*
 * TODO(BohuTANG):
 *	if a key is exists, we will hit a mempool waste,
 *	so should to do some hacks on memeory useage
 */
void msgbuf_put(struct msgbuf *bsm,
                MSN msn,
                msgtype_t type,
                struct msg *key,
                struct msg *val,
                struct txnid_pair *xidpair)
{
	char *base;
	uint32_t sizes = 0U;

	sizes += (sizeof(struct append_entry) + key->size);
	if (type != MSG_DELETE)
		sizes += val->size;
	sizes += sizeof(*xidpair);
	base = mempool_alloc_aligned(bsm->mpool, sizes);
	_encode(base, msn, type, key, val, xidpair);
	skiplist_put(bsm->list, base);
	bsm->count++;
}

/*
 * it's all alloced size, not used size
 */
uint32_t msgbuf_memsize(struct msgbuf *bsm)
{
	return bsm->mpool->memory_used;
}

uint32_t msgbuf_count(struct msgbuf *bsm)
{
	return bsm->count;
}

void msgbuf_free(struct msgbuf *bsm)
{
	if (!bsm) return;

	mempool_free(bsm->mpool);
	skiplist_free(bsm->list);
	xfree(bsm);
}

/*******************************************************
 * msgbuf iterator (thread-safe)
*******************************************************/

void _iter_decode(const char *base, struct msgbuf_iter *bsm_iter)
{
	if (base) {
		_decode((char*)base,
		        &bsm_iter->msn,
		        &bsm_iter->type,
		        &bsm_iter->key,
		        &bsm_iter->val,
		        &bsm_iter->xidpair);
		bsm_iter->valid = 1;
	} else {
		bsm_iter->valid = 0;
	}
}

/* init */
void msgbuf_iter_init(struct msgbuf_iter *bsm_iter, struct msgbuf *bsm)
{
	bsm_iter->valid = 0;
	bsm_iter->bsm = bsm;
	skiplist_iter_init(&bsm_iter->list_iter, bsm->list);
}

/* valid */
int msgbuf_iter_valid(struct msgbuf_iter *bsm_iter)
{
	return skiplist_iter_valid(&bsm_iter->list_iter);
}

/* valid and less or equal */
int msgbuf_iter_valid_lessorequal(struct msgbuf_iter *iter, struct msg *key)
{
	return (skiplist_iter_valid(&iter->list_iter) &&
	        (msg_key_compare(&iter->key, key) <= 0));
}

/* next */
void msgbuf_iter_next(struct msgbuf_iter *bsm_iter)
{
	void *base = NULL;

	skiplist_iter_next(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/* prev */
void msgbuf_iter_prev(struct msgbuf_iter *bsm_iter)
{
	void *base = NULL;

	skiplist_iter_prev(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/*
 * seek
 * when we do msgbuf_iter_seek('key1')
 * the postion in msgbuf is >= postion('key1')
 */
void msgbuf_iter_seek(struct msgbuf_iter *bsm_iter, struct msg *k)
{
	int size;
	char *data;
	void *base = NULL;
	struct append_entry *entry;

	if (!k) return;
	size = (sizeof(struct append_entry) + k->size);
	data = xcalloc(1, size);
	entry = (struct append_entry*)(data);
	entry->keylen = k->size;
	entry->vallen = 0U;
	memcpy(data + sizeof(struct append_entry), k->data, k->size);
	skiplist_iter_seek(&bsm_iter->list_iter, data);
	xfree(data);

	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/* seek to first */
void msgbuf_iter_seektofirst(struct msgbuf_iter *bsm_iter)
{
	void *base = NULL;

	skiplist_iter_seektofirst(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/* seek to last */
void msgbuf_iter_seektolast(struct msgbuf_iter *bsm_iter)
{
	void *base = NULL;

	skiplist_iter_seektolast(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}
