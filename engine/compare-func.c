/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "compare-func.h"

inline int msg_key_compare(struct msg *a, struct msg *b)
{
	if (!a) return -1;
	if (!b) return 1;

	int r;
	uint32_t minlen = a->size < b->size ? a->size : b->size;
	r = memcmp(a->data, b->data, minlen);
	if (r == 0)
		return (a->size - b->size);

	return r;
}

inline int msgbuf_key_compare(void *a, void *b)
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
