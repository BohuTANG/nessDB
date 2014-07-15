/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MB_H_
#define nessDB_MB_H_

#include "internal.h"
#include "pma.h"
#include "msg.h"

#define MSGENTRY_SIZE (sizeof(struct msgentry))
struct msgentry {
	MSN msn;
	uint8_t type;
	struct txnid_pair xidpair;
	uint32_t keylen;
	uint32_t vallen;
} __attribute__((__packed__));

static inline int msgentry_key_compare(void *a, void *b)
{
	int r;

	if (!a) return (-1);
	if (!b) return (+1);

	struct msgentry *mea = (struct msgentry*)a;
	struct msgentry *meb = (struct msgentry*)b;

	uint32_t minlen = mea->keylen < meb->keylen ? mea->keylen : meb->keylen;
	r = memcmp((char*)mea + MSGENTRY_SIZE, (char*)meb + MSGENTRY_SIZE, minlen);
	if (r == 0)
		return (mea->keylen - meb->keylen);
	return r;
}

void msgentry_pack(char*, MSN, msgtype_t, struct msg*, struct msg*, struct txnid_pair*);
void msgentry_unpack(char *, MSN *, msgtype_t *, struct msg *, struct msg *, struct txnid_pair *);

#endif /* nessDB_MB_H_ */
