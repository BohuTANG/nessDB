#ifndef _BLOOM_H
#define _BLOOM_H

typedef unsigned int(*hashfuncs)(const char*);
struct bloom {
	unsigned char *bitset;
	unsigned int size;
	unsigned int count;
	hashfuncs *hashfuncs;
};

struct bloom  *bloom_new(int size);
void bloom_add(struct bloom *bloom, const char *k);
int	bloom_get(struct bloom *bloom, const char *k);
void bloom_free(struct bloom *bloom);

#endif
