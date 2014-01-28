#include "compare.h"

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

int internal_key_compare(void *a, void *b)
{
	TXID atxid;
	TXID btxid;
	struct msg ma;
	struct msg mb;
	register int r;
	register struct fixkey *fk;

	if (!a) return -1;
	if (!b) return +1;
	
	fk = (struct fixkey*)a;
	atxid = fk->txid >> 8;
	ma.size = fk->ksize;
	ma.data = ((char*)a + FIXKEY_SIZE);

	fk = (struct fixkey*)b;
	btxid = fk->txid >> 8;
	mb.size = fk->ksize;
	mb.data = ((char*)b + FIXKEY_SIZE);

	r = msg_key_compare(&ma, &mb);
	if (r == 0) {
		if (atxid > btxid)
			r = -1;
		else if (atxid < btxid)
			r = +1;
	}

	return r;
}


