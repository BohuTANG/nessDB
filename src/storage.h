#ifndef _BTREE_H
#define _BTREE_H


#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include "bitwise.h"
#include "bloom.h"

#define SHA1_LENGTH	20

#define CACHE_SLOTS	23 /* prime */

struct btree_item {
	uint8_t sha1[SHA1_LENGTH];
	__be64 offset;
	__be64 child;
} __attribute__((packed));

#define TABLE_SIZE	((4096 - 1) / sizeof(struct btree_item))

struct btree_table {
	struct btree_item items[TABLE_SIZE];
	uint8_t size;
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
	__be64 free_top;
} __attribute__((packed));

struct btree {
	uint64_t top;
	uint64_t free_top;
	uint64_t alloc;
	uint64_t db_alloc;

	int fd;
	int db_fd;
	struct btree_cache cache[CACHE_SLOTS];
};


//open or creat index&data files
int btree_init(struct btree *btree);


/*
 * Close a database file opened with btree_creat() or btree_open().
 */
void btree_close(struct btree *btree);

/*
 * Insert a new item with key 'sha1' with the contents in 'data' to the
 * database file.
 */
void btree_insert(struct btree *btree, const uint8_t *sha1, const void *data,
		  size_t len);

/*
 * Look up item with the given key 'sha1' in the database file. Length of the
 * item is stored in 'len'. Returns a pointer to the contents of the item.
 * The returned pointer should be released with free() after use.
 */
void *btree_get(struct btree *btree, const uint8_t *sha1, size_t *len,uint64_t *val_offset);
void *btree_get_byoffset(struct btree *btree,uint64_t offset,size_t *len);

/*
 * Remove item with the given key 'sha1' from the database file.
 */
int btree_delete(struct btree *btree, const uint8_t *sha1);

#endif

