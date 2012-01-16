/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _BLOOM_H
#define _BLOOM_H

typedef unsigned int(*hashfuncs)(const char*);
struct bloom {
	unsigned char *bitset;
	unsigned int size;
	unsigned int count;
	hashfuncs *hashfuncs;
};

struct bloom *bloom_new();
void bloom_add(struct bloom *bloom, const char *k);
int bloom_get(struct bloom *bloom, const char *k);
void bloom_free(struct bloom *bloom);

#endif
