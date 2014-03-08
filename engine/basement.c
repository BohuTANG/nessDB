/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "xtypes.h"
#include "compare.h"
#include "basement.h"

void _encode(char *data,
		struct msg *key,
		struct msg *val,
		msgtype_t type,
		MSN msn,
		struct xids *xids)
{
	int pos = 0;
	uint32_t vlen = 0U;
	uint32_t klen = key->size;
	struct append_entry *entry = (struct append_entry*)data;

	if (type != MSG_DELETE)
		vlen = val->size;

	entry->keylen = klen;
	entry->vallen = vlen;
	entry->type = type;
	entry->msn = msn;
	pos += get_entrylen(entry);

	memcpy(data + pos, key->data, key->size);
	pos += key->size;

	if (type != MSG_DELETE) {
		memcpy(data + pos, val->data, val->size);
		pos += val->size;
	}

	memcpy(data + pos, xids, get_xidslen(xids));
}

void _decode(char *data,
		struct msg *key,
		struct msg *val,
		msgtype_t *type,
		MSN *msn,
		struct xids **xids)
{
	(void)xids;
	int pos = 0;
	uint32_t klen;
	uint32_t vlen;
	struct append_entry *entry;

	entry = (struct append_entry*)data;
	klen = entry->keylen;
	vlen = entry->vallen;
	*type = entry->type;
	*msn = entry->msn;
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
	
	*xids = (struct xids*)(data + pos);
}

struct basement *basement_new()
{
	struct basement *bsm;

	bsm = xcalloc(1, sizeof(*bsm));
	bsm->mpool = mempool_new();
	bsm->list = skiplist_new(bsm->mpool, internal_key_compare);
	return bsm;
}

/*
 * TODO(BohuTANG):
 *	if a key is exists, we will hit a mempool waste,
 *	so should to do some hacks on memeory useage
 */
void basement_put(struct basement *bsm,
		struct msg *key,
		struct msg *val,
		msgtype_t type,
		MSN msn,
		struct xids *xids)
{
	char *base;
	uint32_t sizes = 0U;

	sizes += (sizeof(struct append_entry) + key->size);
	if (type != MSG_DELETE) {
		sizes += val->size;
	}
	sizes += get_xidslen(xids);

	base = mempool_alloc_aligned(bsm->mpool, sizes);

	_encode(base, key, val, type, msn, xids);
	skiplist_put(bsm->list, base);
	bsm->count++;
}


/*
 * it's all alloced size, not used size
 */
uint32_t basement_memsize(struct basement *bsm)
{
	return bsm->mpool->memory_used;
}

uint32_t basement_count(struct basement *bsm)
{
	return bsm->count;
}

void basement_free(struct basement *bsm)
{
	if (!bsm)
		return;

	mempool_free(bsm->mpool);
	skiplist_free(bsm->list);
	xfree(bsm);
}

/*******************************************************
 * basement iterator (thread-safe)
*******************************************************/

void _iter_decode(const char *base,
		struct basement_iter *bsm_iter)
{
	if (base) {
		_decode((char*)base,
				&bsm_iter->key,
				&bsm_iter->val,
				&bsm_iter->type,
				&bsm_iter->msn,
				&bsm_iter->xids);
		bsm_iter->valid = 1;
	} else
		bsm_iter->valid = 0;
}

/* init */
void basement_iter_init(struct basement_iter *bsm_iter, struct basement *bsm){
	bsm_iter->valid = 0;
	bsm_iter->bsm = bsm;
	skiplist_iter_init(&bsm_iter->list_iter, bsm->list);
}

/* valid */
int basement_iter_valid(struct basement_iter *bsm_iter)
{
	return skiplist_iter_valid(&bsm_iter->list_iter);
}

/* next */
void basement_iter_next(struct basement_iter *bsm_iter)
{
	void *base = NULL;

	skiplist_iter_next(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/* prev */
void basement_iter_prev(struct basement_iter *bsm_iter)
{
	void *base = NULL;
	
	skiplist_iter_prev(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/*
 * seek
 * when we do basement_iter_seek('key1')
 * the postion in basement is >= postion('key1')
 */
void basement_iter_seek(struct basement_iter *bsm_iter, struct msg *k)
{
	int size;
	char *data;
	void *base = NULL;
	struct append_entry *entry;

	if (!k) return;
	size = (sizeof(struct append_entry) + sizeof(struct xids) + k->size);
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
void basement_iter_seektofirst(struct basement_iter *bsm_iter)
{
	void *base = NULL;
	
	skiplist_iter_seektofirst(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}

/* seek to last */
void basement_iter_seektolast(struct basement_iter *bsm_iter)
{
	void *base = NULL;
	
	skiplist_iter_seektolast(&bsm_iter->list_iter);
	if (bsm_iter->list_iter.node)
		base = bsm_iter->list_iter.node->key;

	_iter_decode(base, bsm_iter);
}
