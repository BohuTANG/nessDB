#ifndef __NESS_BLOOM_H
#define __NESS_BLOOM_H

#include "config.h"

#define NESSDB_BLOOM_BITS (1024 * 64) 

typedef unsigned int(*hashfuncs)(const char *key);

struct bloom {
	unsigned char *bitset;
	unsigned int count;
	hashfuncs *hashfuncs;
};

struct bloom *bloom_new(unsigned char *bitset);
void bloom_add(struct bloom *bloom, const char *key);
int bloom_get(struct bloom *bloom, const char *key);
void bloom_free(struct bloom *bloom);

#endif
