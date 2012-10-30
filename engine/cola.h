#ifndef _COLA_H
#define _COLA_H

#include <stdint.h>
#include "config.h"
#include "bloom.h"
#include "buffer.h"

#define HEADER_SIZE (sizeof(struct cola_header))
#define ITEM_SIZE (sizeof(struct cola_item))

#define MAX_LEVEL (5)
#define L0_SIZE (1024*256)

struct cola_item {
	char data[NESSDB_MAX_KEY_SIZE];
	uint64_t offset;
	uint32_t vlen;
	char opt;
} __attribute__((packed));

struct cola_header {
	int used[MAX_LEVEL];
	int count[MAX_LEVEL];
	char max_key[NESSDB_MAX_KEY_SIZE];
	unsigned char bitset[NESSDB_BLOOM_BITS / 8];
} __attribute__((packed));

struct cola {
	int fd;
	int willfull;
	struct cola_header header;
	struct bloom *bf;
	struct buffer *buf;
};

struct cola *cola_new(const char *file);
int cola_add(struct cola *cola, struct cola_item *item);
void cola_truncate(struct cola *cola);
int cola_isfull(struct cola *cola);
struct cola_item *cola_in_one(struct cola *cola, int *c);
uint64_t cola_get(struct cola *cola, struct slice *sk);
void cola_dump(struct cola *cola);
void cola_free(struct cola *cola);

#endif
