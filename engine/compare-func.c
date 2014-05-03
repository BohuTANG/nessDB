/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "compare-func.h"

int msg_key_compare(struct msg *a, struct msg *b)
{
	if (!a) return -1;
	if (!b) return 1;

	register int r;
	register int minlen;

	minlen = a->size < b->size ? a->size : b->size;
	r = memcmp(a->data, b->data, minlen);
	if (r == 0)
		return (a->size - b->size);

	return r;
}

int msgbuf_key_compare(void *a, void *b, struct cmp_extra *extra)
{
	int pos;
	struct msg ma;
	struct msg mb;

	register int r;
	register struct msg_entry *entry;

	if (!a) return -1;
	if (!b) return +1;

	pos = 0;
	entry = (struct msg_entry*)a;
	ma.size = entry->keylen;
	pos += MSG_ENTRY_SIZE;
	ma.data = ((char*)a + pos);

	pos = 0;
	entry = (struct msg_entry*)b;
	mb.size = entry->keylen;
	pos += MSG_ENTRY_SIZE;
	mb.data = ((char*)b + pos);

	r = msg_key_compare(&ma, &mb);
	if (r == 0) {
		if (extra)
			extra->exists = 1;
	}

	return r;
}
