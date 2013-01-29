#ifndef __nessDB_SST_H
#define __nessDB_SST_H

#include "internal.h"
#include "bloom.h"
#include "block.h"

#define HEADER_SIZE (sizeof(struct sst_header))
#define ITEM_SIZE (sizeof(struct sst_item))

static inline int ness_strcmp(const char *a, const char *b)
{
	return strcmp(a,b);
}

struct ol_pair {
	uint64_t offset;
	uint32_t vlen;
};

struct sst_header {
	uint32_t wasted;
	uint32_t count[MAX_LEVEL];
	char full[MAX_LEVEL];
	char max_key[NESSDB_MAX_KEY_SIZE];
	unsigned char bitset[NESSDB_BLOOM_BITS / 8];
} __attribute__((packed));

struct sst {
	int fd;
	int willfull;
	uint32_t sst_count;
	struct sst_header header;
	struct bloom *bf;
	struct stats *stats;
	struct block *blk;
	struct sst_item *oneblk;
	pthread_mutex_t *lock;
};

struct sst *sst_new(const char *file, struct stats *stats);
int sst_add(struct sst *sst, struct sst_item *item);
int sst_isfull(struct sst *sst);
int sst_get(struct sst *sst, struct slice *sk, struct ol_pair *pair);
void sst_truncate(struct sst *sst);
struct sst_item *sst_in_one(struct sst *sst, int *c);
void sst_dump(struct sst *sst);
void sst_free(struct sst *sst);

#endif
