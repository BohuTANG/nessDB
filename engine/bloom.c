/*
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include "db.h"
#include "bloom.h"
#include "debug.h"
#include "hashs.h"
#include "xmalloc.h"

#if CHAR_BIT != 8
	#define CHAR_BIT (8)
#endif

#define SETBIT_1(bitset, i) (bitset[i / CHAR_BIT] |=  (1<<(i % CHAR_BIT)))
#define SETBIT_0(bitset, i) (bitset[i / CHAR_BIT] &=  (~(1<<(i % CHAR_BIT))))
#define GETBIT(bitset, i) (bitset[i / CHAR_BIT] &   (1<<(i % CHAR_BIT)))
#define HFUNCNUM (2)

struct bloom *bloom_new(unsigned char *bitset)
{
	struct bloom *bl = xcalloc(1, sizeof(struct bloom));

	bl->bitset = bitset;
	bl->hashfuncs = xcalloc(HFUNCNUM, sizeof(hashfuncs));

	bl->hashfuncs[0] = sax_hash;
	bl->hashfuncs[1] = djb_hash;

	return bl;
}

void bloom_add(struct bloom *bloom, const char *key)
{
	int i;
	unsigned int bit;

	for (i = 0; i < HFUNCNUM; i++) {
		bit = bloom->hashfuncs[i](key) % NESSDB_BLOOM_BITS;
		SETBIT_1(bloom->bitset, bit);
	}

	bloom->count++;
}

int bloom_get(struct bloom *bloom, const char *key)
{
	int i;
	unsigned int bit;

	for (i = 0; i < HFUNCNUM; i++) {
		bit = bloom->hashfuncs[i](key) % NESSDB_BLOOM_BITS;
		if (GETBIT(bloom->bitset, bit) == 0)
			return 0;
	}

	return 1;
}

void bloom_free(struct bloom *bloom)
{
	if (bloom) {
		if (bloom->hashfuncs)
			xfree(bloom->hashfuncs);

		xfree(bloom);
	}
}
