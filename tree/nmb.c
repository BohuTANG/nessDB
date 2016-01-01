/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"

static inline int _nmb_entry_key_compare(void *a, void *b, void *extra)
{
	struct nmb *nmb = (struct nmb*)extra;
	struct nmb_entry *nea = (struct nmb_entry*)a;
	struct nmb_entry *neb = (struct nmb_entry*)b;

	return nmb->opts->bt_compare_func((void*)((char*)nea + NMB_ENTRY_SIZE),
	                                  (int)nea->keylen,
	                                  (void*)((char*)neb + NMB_ENTRY_SIZE),
	                                  (int)neb->keylen);
}

void _nmb_entry_pack(char *base,
                     MSN msn,
                     msgtype_t type,
                     struct msg *key,
                     struct msg *val,
                     struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t vlen = 0U;
	uint32_t klen = key->size;
	struct nmb_entry *entry = (struct nmb_entry*)base;

	if (type != MSG_DELETE)
		vlen = val->size;

	entry->msn = msn;
	entry->type = type;
	entry->xidpair = *xidpair;
	entry->keylen = klen;
	entry->vallen = vlen;
	pos += NMB_ENTRY_SIZE;

	memcpy(base + pos, key->data, key->size);
	pos += key->size;
	if (type != MSG_DELETE)
		memcpy(base + pos, val->data, val->size);
}

void _nmb_entry_unpack(char *base,
                       MSN *msn,
                       msgtype_t *type,
                       struct msg *key,
                       struct msg *val,
                       struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t klen;
	uint32_t vlen;
	struct nmb_entry *entry;

	entry = (struct nmb_entry*)base;
	*msn = entry->msn;
	*type = entry->type;
	*xidpair = entry->xidpair;
	klen = entry->keylen;
	vlen = entry->vallen;
	pos += NMB_ENTRY_SIZE;

	key->size = klen;
	key->data = (base + pos);
	pos += klen;
	if (*type != MSG_DELETE) {
		val->size = vlen;
		val->data = (base + pos);
	} else {
		memset(val, 0, sizeof(*val));
	}
}

struct nmb *nmb_new(struct tree_options *opts)
{
	struct nmb *nmb = xmalloc(sizeof(*nmb));

	nmb->mpool = mempool_new();
	nmb->pma = pma_new(64);
	nmb->count = 0;
	nmb->opts = opts;

	return nmb;
}

void nmb_free(struct nmb *nmb)
{
	if (!nmb)
		return;

	pma_free(nmb->pma);
	mempool_free(nmb->mpool);
	xfree(nmb);
}

uint32_t nmb_memsize(struct nmb *nmb)
{
	return nmb->mpool->memory_used;
}

uint32_t nmb_count(struct nmb *nmb)
{
	return nmb->count;
}

void nmb_put(struct nmb *nmb,
             MSN msn,
             msgtype_t type,
             struct msg *key,
             struct msg *val,
             struct txnid_pair *xidpair)
{
	char *base;
	uint32_t size = (NMB_ENTRY_SIZE + key->size);

	if (type != MSG_DELETE)
		size += val->size;

	base = mempool_alloc_aligned(nmb->mpool, size);
	_nmb_entry_pack(base, msn, type, key, val, xidpair);
	pma_insert(nmb->pma,
	           (void*)base,
	           _nmb_entry_key_compare,
	           (void*)nmb);
	nmb->count++;
}

/*
 * EFFECTS:
 *	- unpack iter->base to nmb_values
 */
void nmb_get_values(struct mb_iter *iter, struct nmb_values *values)
{
	_nmb_entry_unpack(iter->base,
	                  &values->msn,
	                  &values->type,
	                  &values->key,
	                  &values->val,
	                  &values->xidpair);
}


int nmb_pack_values_to_msgpack(struct nmb_values *values, struct msgpack *packer)
{
	MSN msn = values->msn;

	msn = ((msn << 8) | values->type);
	msgpack_pack_uint64(packer, msn);
	msgpack_pack_uint64(packer, values->xidpair.child_xid);
	msgpack_pack_uint64(packer, values->xidpair.parent_xid);
	msgpack_pack_uint32(packer, values->key.size);
	msgpack_pack_nstr(packer, values->key.data, values->key.size);

	if (values->type != MSG_DELETE) {
		msgpack_pack_uint32(packer, values->val.size);
		msgpack_pack_nstr(packer, values->val.data, values->val.size);
	}

	return NESS_OK;
}

/*
 * EFFECTS:
 *	- unpack nmb values from msgpack
 * RETURNS:
 *	- 1: succuess
 *	- 0: fail
 */
int nmb_unpack_values_from_msgpack(struct msgpack *packer, struct nmb_values *values)
{
	if (!msgpack_unpack_uint64(packer, &values->msn)) goto ERR;
	values->type = (values->msn & 0xff);
	values->msn = (values->msn >> 8);
	if (!msgpack_unpack_uint64(packer, &values->xidpair.child_xid)) goto ERR;
	if (!msgpack_unpack_uint64(packer, &values->xidpair.parent_xid)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &values->key.size)) goto ERR;
	if (!msgpack_unpack_nstr(packer, values->key.size, (char**)&values->key.data)) goto ERR;
	if (values->type != MSG_DELETE) {
		if (!msgpack_unpack_uint32(packer, &values->val.size)) goto ERR;
		if (!msgpack_unpack_nstr(packer, values->val.size, (char**)&values->val.data)) goto ERR;
	}

	return NESS_OK;
ERR:
	return NESS_ERR;
}

void nmb_to_msgpack(void *p, void *n)
{
	struct mb_iter iter;
	struct msgpack *packer = (struct msgpack*)p;
	struct nmb *nmb = (struct nmb*)n;

	mb_iter_init(&iter, nmb->pma);
	while (mb_iter_next(&iter)) {
		struct nmb_values values;

		nmb_get_values(&iter, &values);
		nmb_pack_values_to_msgpack(&values, packer);
	}
}

void msgpack_to_nmb(void *p, void *n)
{
	struct msgpack *packer = (struct msgpack*)p;
	struct nmb *nmb = (struct nmb*)n;
	while (packer->SEEK < packer->NUL) {
		struct nmb_values values;

		nmb_unpack_values_from_msgpack(packer, &values);
		nmb_put(nmb,
		        values.msn,
		        values.type,
		        &values.key,
		        &values.val,
		        &values.xidpair);
	}
}

/*
 * EFFECTS:
 *	return the first coord which > k
 */
int nmb_get_left_coord(struct nmb *nmb, struct msg *left, struct pma_coord *coord)
{
	memset(coord, 0, sizeof(*coord));
	if (left) {
		void *retval;
		struct nmb_entry *entry = xcalloc(1, NMB_ENTRY_SIZE + left->size);

		entry->keylen = left->size;
		xmemcpy((char*)entry + NMB_ENTRY_SIZE, left->data, left->size);
		pma_find_plus(nmb->pma,
		              entry,
		              _nmb_entry_key_compare,
		              nmb,
		              &retval, coord);
		xfree(entry);
	}

	return 1;
}

/*
 * EFFECTS:
 *	return the first coord which < k
 */
int nmb_get_right_coord(struct nmb *nmb, struct msg *right, struct pma_coord *coord)
{
	int slot_idx = nmb->pma->used - 1;
	/* is uesd not used - 1 */
	int array_idx = nmb->pma->slots[slot_idx]->used;

	coord->slot_idx = slot_idx;
	coord->array_idx = array_idx;
	if (right) {
		void *retval;
		struct nmb_entry *entry = xcalloc(1, NMB_ENTRY_SIZE + right->size);

		entry->keylen = right->size;
		xmemcpy((char*)entry + NMB_ENTRY_SIZE, right->data, right->size);
		pma_find_plus(nmb->pma,
		              entry,
		              _nmb_entry_key_compare,
		              nmb,
		              &retval,
		              coord);
		xfree(entry);
	}

	return 1;
}
