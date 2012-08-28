/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include <stdlib.h>
#include <stdarg.h>
#include "util.h"
#include "config.h"
#include "bloom.h"
#include "debug.h"

#define HFUNCNUM (2)

struct bloom *bloom_new()
{
	struct bloom *bl = calloc(1, sizeof(struct bloom));

	if (!bl) 
		__PANIC("bl is NULL when bloom_new, will abort...");

	bl->size = BLOOM_BITS;
	bl->bitset = calloc((bl->size + 1) / sizeof(char), sizeof(char));
	if (!bl->bitset)
		__PANIC("bl->bitset is NULL , will abort...bl->size is %lu", bl->size);

	bl->hashfuncs = calloc(HFUNCNUM, sizeof(hashfuncs));
	if (!bl->hashfuncs)
		__PANIC("bl->hashfunc is NULL , will abort...");

	bl->hashfuncs[0] = sax_hash;
	bl->hashfuncs[1] = djb_hash;

	return bl;
}

void bloom_add(struct bloom *bloom, const char *k)
{
	int i;
	unsigned int bit;

	if (!k)
		return;

	for (i = 0; i < HFUNCNUM; i++) {
		bit = bloom->hashfuncs[i](k) % bloom->size;
		SETBIT_1(bloom->bitset, bit);
	}

	bloom->count++;
}

int bloom_get(struct bloom *bloom, const char *k)
{
	int i;
	unsigned int bit;

	if (!k)
		return 0;

	for (i = 0; i < HFUNCNUM; i++) {
		bit = bloom->hashfuncs[i](k) % bloom->size;
		if (GETBIT(bloom->bitset, bit) == 0)
			return 0;
	}

	return 1;
}

void bloom_free(struct bloom *bloom)
{
	if (bloom) {
		if (bloom->bitset)
			free(bloom->bitset);

		if(bloom->hashfuncs)
			free(bloom->hashfuncs);

		free(bloom);
	}
}
