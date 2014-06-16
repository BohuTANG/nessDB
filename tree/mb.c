/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "mb.h"

void msgentry_pack(char *base,
                   MSN msn,
                   msgtype_t type,
                   struct msg *key,
                   struct msg *val,
                   struct txnid_pair *xidpair)
{
	int pos = 0;
	uint32_t vlen = 0U;
	uint32_t klen = key->size;
	struct msgentry *entry = (struct msgentry*)base;

	if (type != MSG_DELETE)
		vlen = val->size;

	entry->msn = msn;
	entry->type = type;
	entry->xidpair = *xidpair;
	entry->keylen = klen;
	entry->vallen = vlen;
	pos += MSGENTRY_SIZE;

	memcpy(base + pos, key->data, key->size);
	pos += key->size;
	if (type != MSG_DELETE)
		memcpy(base + pos, val->data, val->size);
}

void msgentry_unpack(char *base,
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

	entry = (struct msgentry*)base;
	*msn = entry->msn;
	*type = entry->type;
	*xidpair = entry->xidpair;
	klen = entry->keylen;
	vlen = entry->vallen;
	pos += MSGENTRY_SIZE;

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
