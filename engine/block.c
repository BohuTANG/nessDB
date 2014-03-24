/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "block.h"

/* auto extend pairs array size */
void _extend(struct block *b, uint32_t n)
{
	DISKOFF size = b->pairs_used + n;

	if (size >= b->pairs_size) {
		DISKOFF atleast = b->pairs_size * 2;

		if (size < atleast)
			size = atleast;
		b->pairs = xrealloc(b->pairs, size * sizeof(struct block_pair));
		b->pairs_size = size;
	}
}

static int _pair_compare_fun(const void *a, const void *b)
{
	struct block_pair *pa = (struct block_pair*)a;
	struct block_pair *pb = (struct block_pair*)b;

	if (pa->offset > pb->offset)
		return 1;
	else if (pa->offset < pb->offset)
		return -1;

	return 0;
}

struct block *block_new() {
	struct block *b;

	b = xcalloc(1, sizeof(*b));
	rwlock_init(&b->rwlock);
	b->allocated += ALIGN(BLOCK_OFFSET_START);

	return b;
}


void block_init(struct block *b,
                struct block_pair *pairs,
                uint32_t n)
{
	uint32_t i;

	nassert(n > 0);

	write_lock(&b->rwlock);
	qsort(pairs, n, sizeof(*pairs), _pair_compare_fun);
	for (i = 0; i < n; i++) {
		_extend(b, 1);
		memcpy(&b->pairs[i], &pairs[i], sizeof(*pairs));
		b->pairs_used++;
	}

	/* get the last allocated postion of file*/
	b->allocated += pairs[n - 1].offset;
	b->allocated += ALIGN(pairs[n - 1].real_size);
	write_unlock(&b->rwlock);
}

DISKOFF block_alloc_off(struct block *b,
                        uint64_t nid,
                        uint32_t real_size,
                        uint32_t skeleton_size,
                        uint32_t height)
{
	DISKOFF r;
	uint32_t i;
	int found = 0;
	uint32_t pos = 0;

	write_lock(&b->rwlock);
	r = ALIGN(b->allocated);
	_extend(b, 1);

	/*
	 * set old hole to fly
	 * it is not visible until you call 'block_shrink'
	 */
	for (i = 0; i < b->pairs_used; i++) {
		if (b->pairs[i].nid == nid) {
			b->pairs[i].used = 0;
			break;
		}
	}

	/* find the not-fly hole to reuse */
	if (b->pairs_used > 0) {
		for (pos = 0; pos < (b->pairs_used - 1); pos++) {
			DISKOFF off_aligned;
			struct block_pair *p;
			struct block_pair *nxtp;

			p = &b->pairs[pos];
			nxtp = &b->pairs[pos + 1];
			off_aligned = (ALIGN(p->offset) + ALIGN(p->real_size));
			if ((off_aligned + ALIGN(real_size)) <= nxtp->offset) {
				r = off_aligned;
				memmove(&b->pairs[pos + 1 + 1],
				        &b->pairs[pos + 1],
				        sizeof(*b->pairs) * (b->pairs_used - pos - 1));
				found = 1;
				break;
			}
		}
	}

	/* found the reuse hole */
	if (found) {
		pos += 1;
	} else {
		pos = b->pairs_used;
		b->allocated = (ALIGN(b->allocated) +  ALIGN(real_size));
	}

	b->pairs[pos].offset = r;
	b->pairs[pos].height = height;
	b->pairs[pos].real_size = real_size;
	b->pairs[pos].skeleton_size = skeleton_size;
	b->pairs[pos].nid = nid;
	b->pairs[pos].used = 1;

	b->pairs_used++;
	write_unlock(&b->rwlock);

	return r;
}

int block_get_off_bynid(struct block *b,
                        uint64_t nid,
                        struct block_pair **bpair)
{
	uint32_t i;
	int rval = NESS_ERR;

	read_lock(&b->rwlock);
	for (i = 0; i < b->pairs_used; i++) {
		if (b->pairs[i].nid == nid) {
			if (b->pairs[i].used) {
				*bpair = &b->pairs[i];
				rval = NESS_OK;
				break;
			}
		}
	}
	read_unlock(&b->rwlock);

	return rval;
}

/*
 * REQUIRES:
 *	block write lock
 *
 * EFFECTS:
 *	empower the fly holes to be visiable
 */
void block_shrink(struct block *b)
{
	uint32_t i, j;

	write_lock(&b->rwlock);
	for (i = 0, j = 0; i < b->pairs_used; i++) {
		if (b->pairs[i].used) {
			b->pairs[j++] = b->pairs[i];
		}
	}
	b->pairs_used = j;
	write_unlock(&b->rwlock);
}

void block_free(struct block *b)
{
	if (!b) return;

	rwlock_destroy(&b->rwlock);
	xfree(b->pairs);
	xfree(b);
}
