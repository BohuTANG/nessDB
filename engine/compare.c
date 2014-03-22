#include "internal.h"
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
	int pos;
	MSN msna;
	MSN msnb;
	struct msg ma;
	struct msg mb;

	register int r;
	register struct append_entry *entry;

	if (!a) return -1;
	if (!b) return +1;
	
	pos = 0;
	entry = (struct append_entry*)a;
	msna = entry->msn;
	ma.size = entry->keylen;
	pos += get_entrylen(entry);
	ma.data = ((char*)a + pos);

	pos = 0;
	entry = (struct append_entry*)b;
	msnb = entry->msn;
	mb.size = entry->keylen;
	pos += get_entrylen(entry);
	mb.data = ((char*)b + pos);

	r = msg_key_compare(&ma, &mb);

	/* TODO: (BohuTANG) compare msn */
	if (r == 0) {
		/*
		if (msna > msnb)
			r = -1;
		else if (msna < msnb)
			r = +1;
		*/
		(void)msna;
		(void)msnb;
	}

	return r;
}
