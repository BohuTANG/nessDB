#ifndef _BTREE_H
#define _BTREE_H

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include "bitwise.h"

#include "platform.h"
#include "util.h"
#include "buffer.h"

#define SHA1_LENGTH	(32)

struct btree_item {
	char sha1[SHA1_LENGTH];
	__be64 offset;
	__be64 child;
} __attribute__((packed));

#define TABLE_SIZE	((4096 - 1) / sizeof(struct btree_item))

struct btree_table {
	uint16_t size;
	struct btree_item items[TABLE_SIZE];	
} __attribute__((packed));

struct btree_cache {
	uint64_t offset;
	struct btree_table *table;
};

struct blob_info {
	__be32 len;
};

struct btree_super {
	__be64 top;
} __attribute__((packed));

#define CACHE_SLOTS (6151)
  	
struct btree {
	uint64_t top;
	uint64_t alloc;
	uint64_t db_alloc;
	uint64_t super_top;

	int fd;
	int db_fd;
	int slot_prime;
	struct btree_cache cache[CACHE_SLOTS];
	struct buffer *buf;
};

int btree_init(struct btree *btree,const char *db);
void btree_close(struct btree *btree);
void btree_insert_index(struct btree *btree, const char *sha1, uint64_t  data_offset);
uint64_t btree_insert_data(struct btree *btree, const void *data,size_t len);
void *btree_get(struct btree *btree, const char *sha1);
int btree_get_index(struct btree *btree, const char *sha1);
struct slice *btree_get_data(struct btree *btree,uint64_t offset);
int btree_delete(struct btree *btree, const char *sha1);

#endif

