/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "basement.h"

void _encode_kv(char *data,
		struct msg *key,
		struct msg *val,
		msgtype_t type)
{
	int pos = 0;
	uint32_t vsize = 0U;
	uint32_t ksize = key->size;
	struct fixkey *fk;

	if (type != MSG_DEL)
		vsize = val->size;

	fk = (struct fixkey*)(data);
	fk->ksize = ksize;
	fk->vsize = vsize;
	fk->type = type;
	pos += FIXKEY_SIZE;

	memcpy(data + pos, key->data, key->size);
	pos += key->size;

	if (type != MSG_DEL) {
		memcpy(data + pos, val->data, val->size);
	}
}

void _decode_kv(char *data,
		struct msg *key,
		struct msg *val,
		msgtype_t *type)
{
	int pos = 0;
	uint32_t ksize;
	uint32_t vsize;
	struct fixkey *fk;
	
	fk = (struct fixkey*)data;
	ksize = fk->ksize;
	vsize = fk->vsize;
	*type = fk->type;
	pos += FIXKEY_SIZE;

	key->size = ksize;
	key->data = (data + pos);
	pos += ksize;

	if (*type != MSG_DEL) {
		val->size = vsize;
		val->data = (data + pos);
	} else {
		memset(val, 0, sizeof(*val));
	}
}

int _bsm_compare_fun(void *a, void *b)
{
	struct msg ma;
	struct msg mb;
	struct fixkey *fk;

	if (!a) return -1;
	if (!b) return +1;
	
	fk = (struct fixkey*)a;
	ma.size = fk->ksize;
	ma.data = ((char*)a + FIXKEY_SIZE);

	fk = (struct fixkey*)b;
	mb.size = fk->ksize;
	mb.data = ((char*)b + FIXKEY_SIZE);

	return msgcmp(&ma, &mb);
}

judgetype_t _bsm_judge_fun(void *a, void *b)
{
	struct msg ma;
	struct msg mb;
	struct fixkey *fk;

	if ((!a) || (!b)) return J_PUT;
	
	fk = (struct fixkey*)a;
	ma.size = fk->ksize;
	ma.data = ((char*)a + FIXKEY_SIZE);

	fk = (struct fixkey*)b;
	mb.size = fk->ksize;
	mb.data = ((char*)b + FIXKEY_SIZE);

	/* if a == b, we just overwite it in skiplist */
	if (msgcmp(&ma, &mb) == 0)
		return J_OVERWRITE;

	return J_PUT;
}

struct basement *basement_new()
{
	struct basement *bsm;

	bsm = xcalloc(1, sizeof(*bsm));
	bsm->mpool = mempool_new();
	bsm->list = skiplist_new(bsm->mpool,
			&_bsm_judge_fun,
			&_bsm_compare_fun);

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
		msgtype_t type)
{
	char *base;
	uint32_t sizes = 0U;

	sizes += (sizeof(struct fixkey) + key->size);
	if (type != MSG_DEL) {
		sizes += val->size;
	}
	base = mempool_alloc_aligned(bsm->mpool, sizes);

	_encode_kv(base,
			key,
			val,
			type);
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
		_decode_kv((char*)base,
				&bsm_iter->key,
				&bsm_iter->val,
				&bsm_iter->type);
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
	char *fixkey;
	void *base = NULL;
	struct fixkey *fk;

	if (!k) return;

	size = (FIXKEY_SIZE + k->size);
	fixkey = xcalloc(1, size);

	fk = (struct fixkey*)(fixkey);
	fk->ksize = k->size;
	fk->vsize = 0U;
	fk->type = MSG_PUT;
	memcpy(fixkey + FIXKEY_SIZE, k->data, k->size);
	skiplist_iter_seek(&bsm_iter->list_iter, fixkey);
	xfree(fixkey);

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
