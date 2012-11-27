#ifndef __nessDB_COLA_H
#define __nessDB_COLA_H

#include "db.h"
#include "bloom.h"
#include "compact.h"
#include "block.h"

#define HEADER_SIZE (sizeof(struct cola_header))
#define ITEM_SIZE (sizeof(struct cola_item))

struct ol_pair {
	uint64_t offset;
	uint32_t vlen;
};

struct cola_header {
	int count[MAX_LEVEL];
	char max_key[NESSDB_MAX_KEY_SIZE];
	unsigned char bitset[NESSDB_BLOOM_BITS / 8];
} __attribute__((packed));

struct cola {
	int fd;
	int willfull;
	int sst_count;
	struct cola_header header;
	struct bloom *bf;
	struct compact *cpt;
	struct stats *stats;
	struct block *blk;
	struct cola_item *oneblk;
};

struct cola *cola_new(const char *file, struct compact *cpt, struct stats *stats);
STATUS cola_add(struct cola *cola, struct cola_item *item);
STATUS cola_isfull(struct cola *cola);
STATUS cola_get(struct cola *cola, struct slice *sk, struct ol_pair *pair);
void cola_truncate(struct cola *cola);
struct cola_item *cola_in_one(struct cola *cola, int *c);
void cola_dump(struct cola *cola);
void cola_free(struct cola *cola);

#endif
