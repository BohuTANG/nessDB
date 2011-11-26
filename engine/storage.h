#ifndef _BTREE_H
#define _BTREE_H

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include "bitwise.h"
#include "platform.h"

#define SHA1_LENGTH	(20)

struct btree_item {
	char sha1[SHA1_LENGTH];
	__be32 offset;
	__be32 child;
} __attribute__((packed));

#define TABLE_SIZE	((4096 - 1) / sizeof(struct btree_item))

struct btree_table {
	uint16_t size;
	struct btree_item items[TABLE_SIZE];	
} __attribute__((packed));

struct btree_cache {
	uint32_t offset;
	struct btree_table *table;
};

struct blob_info {
	__be32 len;
};

struct btree_super {
	__be32 top;
	__be32 free_top;
} __attribute__((packed));

#define CACHE_SLOTS (6151)
  	
struct btree {
	UINT top;
	UINT free_top;
	UINT alloc;
	UINT db_alloc;

	int fd;
	int db_fd;
	int slot_prime;
	struct btree_cache cache[CACHE_SLOTS];
};


//open or creat index&data files
int btree_init(struct btree *btree,const char *db);


/*
 * Close a database file opened with btree_creat() or btree_open().
 */
void btree_close(struct btree *btree);

void btree_insert_index(struct btree *btree, const char *sha1, UINT  data_offset);

UINT btree_insert_data(struct btree *btree, const void *data,size_t len);

/*
 * Look up item with the given key 'sha1' in the database file. Length of the
 * item is stored in 'len'. Returns a pointer to the contents of the item.
 * The returned pointer should be released with free() after use.
 */
void *btree_get(struct btree *btree, const char *sha1);
int btree_get_index(struct btree *btree, const char *sha1);
void *btree_get_byoffset(struct btree *btree,uint32_t offset);

/*
 * Remove item with the given key 'sha1' from the database file.
 */
int btree_delete(struct btree *btree, const char *sha1);

#endif

