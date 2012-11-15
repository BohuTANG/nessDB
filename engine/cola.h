#ifndef _COLA_H
#define _COLA_H

#include "config.h"
#include "bloom.h"
#include "compact.h"

#define HEADER_SIZE (sizeof(struct cola_header))
#define ITEM_SIZE (sizeof(struct cola_item))

#define MAX_LEVEL (4)
#define L0_SIZE (1024)
#define NESSDB_MAX_KEY_SIZE (35) 

struct ol_pair {
	uint64_t offset;
	uint32_t vlen;
};

struct cola_item {
	char data[NESSDB_MAX_KEY_SIZE];
	uint64_t offset;
	uint32_t vlen;
	char opt;
} __attribute__((packed));

struct cola_header {
	int count[MAX_LEVEL];
	char max_key[NESSDB_MAX_KEY_SIZE];
	unsigned char bitset[NESSDB_BLOOM_BITS / 8];
} __attribute__((packed));

struct cola {
	int fd;
	int allcount;
	int willfull;
	struct cola_header header;
	struct bloom *bf;
	struct compact *cpt;
};

struct cola *cola_new(const char *file, struct compact *cpt);
STATUS cola_add(struct cola *cola, struct cola_item *item);
STATUS cola_isfull(struct cola *cola);
STATUS cola_get(struct cola *cola, struct slice *sk, struct ol_pair *pair);
void cola_truncate(struct cola *cola);
struct cola_item *cola_in_one(struct cola *cola, int *c);
void cola_dump(struct cola *cola);
void cola_free(struct cola *cola);

#endif
