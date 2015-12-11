/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"

static inline int _lmb_entry_key_compare(void *a, void *b, void *extra)
{
	struct lmb *lmb = (struct lmb*)extra;
	struct leafentry *lea = (struct leafentry*)a;
	struct leafentry *leb = (struct leafentry*)b;

	return lmb->opts->bt_compare_func(lea->keyp,
	                                  lea->keylen,
	                                  leb->keyp,
	                                  leb->keylen);
}

struct lmb *lmb_new(struct tree_options *opts)
{
	struct lmb *lmb = xmalloc(sizeof(*lmb));

	lmb->mpool = mempool_new();
	lmb->pma = pma_new(64);
	lmb->opts = opts;
	lmb->count = 0;

	return lmb;
}

void lmb_put(struct lmb *lmb,
             MSN msn,
             msgtype_t type,
             struct msg *key,
             struct msg *val,
             struct txnid_pair *xidpair)
{
	(void)msn;
	char *base;
	uint32_t size;
	struct leafentry *le = NULL;
	struct pma_coord coord;

	struct leafentry kle = {.keylen = key->size, .keyp = key->data};
	int ret = pma_find_zero(lmb->pma,
	                        (void*)&kle,
	                        _lmb_entry_key_compare,
	                        (void*)lmb,
	                        (void**)&le,
	                        &coord);

	if (ret == NESS_NOTFOUND) {
		size = (LEAFENTRY_SIZE + key->size);
		base = mempool_alloc_aligned(lmb->mpool, size);
		le = (struct leafentry*)base;
		memset(le, 0, LEAFENTRY_SIZE);

		le->keylen = key->size;
		le->keyp = base + LEAFENTRY_SIZE;

		xmemcpy(le->keyp, key->data, key->size);
		pma_insert(lmb->pma,
		           (void*)base,
		           _lmb_entry_key_compare,
		           (void*)lmb);
		lmb->count++;
	}

	/* alloc value in mempool arena */
	if (!le->xrs) {
		le->xrs_size = 1;
		le->xrs = (struct xr*)mempool_alloc_aligned(lmb->mpool, le->xrs_size * sizeof(struct xr));
	} else {
		int size = le->xrs_size;
		int nums = (le->num_pxrs + le->num_cxrs);

		if (nums == size) {
			if (size < 8)
				size *= 2;
			else
				size += 2;
		}
		base = mempool_alloc_aligned(lmb->mpool, sizeof(struct xr) * size);
		xmemcpy(base, le->xrs, sizeof(struct xr) * nums);
		le->xrs = (struct xr*)base;
		le->xrs_size = size;
	}
	struct xr *xr = &le->xrs[le->num_pxrs + le->num_cxrs];

	xr->type = type;
	xr->xid = xidpair->parent_xid;
	if (type != MSG_DELETE) {
		base = mempool_alloc_aligned(lmb->mpool, val->size);
		xmemcpy(base, val->data, val->size);
		xr->vallen = val->size;
		xr->valp = base;
	}
	le->num_pxrs++;
}

uint32_t lmb_memsize(struct lmb *lmb)
{
	return lmb->mpool->memory_used;
}

uint32_t lmb_count(struct lmb *lmb)
{
	return lmb->count;
}

void lmb_free(struct lmb *lmb)
{
	if (!lmb)
		return;

	pma_free(lmb->pma);
	mempool_free(lmb->mpool);
	xfree(lmb);
}

/*
 * EFFECTS:
 *	- clone one leafentry to another one who malloced in mpool
 */
static void _lmb_leafentry_clone(struct leafentry *le,
                                 struct mempool *mpool,
                                 struct leafentry **le_cloned)
{
	int i;
	char *base;
	struct leafentry *new_le;
	uint32_t size = (LEAFENTRY_SIZE + le->keylen);
	int nums = le->num_pxrs + le->num_cxrs;

	base = mempool_alloc_aligned(mpool, size);
	new_le = (struct leafentry*)base;
	memset(new_le, 0, LEAFENTRY_SIZE);

	new_le->keylen = le->keylen;
	new_le->keyp = base + LEAFENTRY_SIZE;
	new_le->xrs = (struct xr*)mempool_alloc_aligned(mpool, nums * XR_SIZE);

	/* copy key bytestring to mempool arena */
	xmemcpy(new_le->keyp, le->keyp, le->keylen);
	for (i = 0; i < nums; i++) {
		size = le->xrs[i].vallen;
		base = mempool_alloc_aligned(mpool, size);
		xmemcpy(base, le->xrs[i].valp, size);

		new_le->xrs[i] = le->xrs[i];
		new_le->xrs[i].valp = base;
	}

	nassert(new_le);
	*le_cloned = new_le;
}

/*
 * EFFECTS:
 *	- split lmb to lmba & lmbb in evenly
 *	- the old lmb need lmb_free
 */
void lmb_split(struct lmb *lmb,
               struct lmb **lmba,
               struct lmb **lmbb,
               struct msg **split_key)
{
	uint32_t i = 0;
	struct mb_iter iter;
	uint32_t count = lmb_count(lmb);
	uint32_t a_count = count / 2;
	struct lmb *A = lmb_new(lmb->opts);
	struct lmb *B = lmb_new(lmb->opts);

	nassert(count > 1);
	mb_iter_init(&iter, lmb->pma);
	while (mb_iter_next(&iter)) {
		struct lmb *mb;
		struct leafentry *le;
		struct leafentry *le_clone = NULL;

		le = (struct leafentry*)iter.base;
		nassert(le);

		/* TODO(BohuTANG): count is 2 */
		if (i <= a_count) {
			mb = A;
			if (nessunlikely(i == a_count)) {
				struct msg spkey = { .size = le->keylen, .data = le->keyp};
				*split_key = msgdup(&spkey);
			}
		} else {
			mb = B;
		}

		/* append to pma */
		_lmb_leafentry_clone(le, mb->mpool, &le_clone);
		pma_append(mb->pma, le_clone, _lmb_entry_key_compare, mb);
		mb->count++;

		i++;
	}

	nassert(*split_key);
	*lmba = A;
	*lmbb = B;
}

static int lmb_pack_le_to_msgpack(struct leafentry *le, struct msgpack *packer)
{
	uint32_t i;
	uint32_t nums;

	/* 1) key */
	msgpack_pack_uint32(packer, le->keylen);
	msgpack_pack_nstr(packer, le->keyp, le->keylen);

	/* 2) xrs num */
	msgpack_pack_uint32(packer, le->num_pxrs);
	msgpack_pack_uint32(packer, le->num_cxrs);

	/* 3) xrs */
	nums = le->num_pxrs + le->num_cxrs;
	for (i = 0; i < nums; i++) {
		struct xr *xr = &le->xrs[i];

		msgpack_pack_uint64(packer, xr->xid);
		msgpack_pack_uint8(packer, xr->type);
		msgpack_pack_uint32(packer, xr->vallen);
		msgpack_pack_nstr(packer, xr->valp, xr->vallen);
	}

	return NESS_OK;
}


static int lmb_unpack_le_from_msgpack(struct msgpack *packer, struct leafentry *le)
{
	uint32_t i;
	uint32_t nums;

	memset(le, 0, LEAFENTRY_SIZE);
	/* 1) key */
	if (!msgpack_unpack_uint32(packer, &le->keylen)) goto ERR;
	if (msgpack_unpack_nstr(packer, le->keylen, (char**)&le->keyp)) goto ERR;

	/* 2) xrs num */
	if (msgpack_unpack_uint32(packer, &le->num_pxrs)) goto ERR;
	if (msgpack_unpack_uint32(packer, &le->num_cxrs)) goto ERR;

	/* 3) xrs */
	nums = le->num_pxrs + le->num_cxrs;
	for (i = 0; i < nums; i++) {
		struct xr *xr = &le->xrs[i];

		if (!msgpack_unpack_uint64(packer, &xr->xid)) goto ERR;
		if (!msgpack_unpack_uint8(packer, (uint8_t*)&xr->type)) goto ERR;
		if (!msgpack_unpack_uint32(packer, &xr->vallen)) goto ERR;
		if (!msgpack_unpack_nstr(packer, xr->vallen, (char**)&xr->valp)) goto ERR;
	}

	return NESS_OK;
ERR:
	return NESS_ERR;
}

/*
 * EFFECTS:
 *	- pack lmb leafentry to msgpack
 */
void lmb_to_msgpack(void *p, void *n)
{
	struct mb_iter iter;
	struct msgpack *packer = (struct msgpack*)p;
	struct lmb *lmb = (struct lmb*)n;

	mb_iter_init(&iter, lmb->pma);
	while (mb_iter_next(&iter)) {
		struct leafentry *le = (struct leafentry*)iter.base;

		lmb_pack_le_to_msgpack(le, packer);
	}
}

/*
 * EFFECTS:
 *	- unpack lmb leafentry from msgpack
 */
void msgpack_to_lmb(void *p, void *n)
{
	struct msgpack *packer = (struct msgpack*)p;
	struct lmb *lmb = (struct lmb*)n;

	while (packer->SEEK < packer->NUL) {
		struct leafentry le;
		struct leafentry *new_le;

		lmb_unpack_le_from_msgpack(packer, &le);

		/* clone le to mpool */
		_lmb_leafentry_clone(&le, lmb->mpool, &new_le);
		pma_append(lmb->pma,
		           (void*)new_le,
		           _lmb_entry_key_compare,
		           (void*)lmb);
		lmb->count++;
	}
}

/*
 * EFFECTS:
 *	find the entry which = key
 */
int lmb_find_zero(struct lmb *lmb,
                  struct msg *key,
                  struct leafentry **le,
                  struct pma_coord *coord)
{
	struct leafentry kle = {.keylen = key->size, .keyp = key->data};

	return pma_find_zero(lmb->pma,
	                     (void*)&kle,
	                     _lmb_entry_key_compare,
	                     (void*)lmb,
	                     (void**)le,
	                     coord);
}

/*
 * EFFECTS:
 *	find the first entry which > key
 */
int lmb_find_plus(struct lmb *lmb,
                  struct msg *key,
                  struct leafentry **le,
                  struct pma_coord *coord)
{
	struct leafentry kle = {.keylen = key->size, .keyp = key->data};

	return pma_find_plus(lmb->pma,
	                     (void*)&kle,
	                     _lmb_entry_key_compare,
	                     (void*)lmb,
	                     (void**)le,
	                     coord);
}

/*
 * EFFECTS:
 *	find the first entry which < key
 */
int lmb_find_minus(struct lmb *lmb,
                   struct msg *key,
                   struct leafentry **le,
                   struct pma_coord *coord)
{
	struct leafentry kle = {.keylen = key->size, .keyp = key->data};

	return pma_find_minus(lmb->pma,
	                      (void*)&kle,
	                      _lmb_entry_key_compare,
	                      (void*)lmb,
	                      (void**)le,
	                      coord);
}
