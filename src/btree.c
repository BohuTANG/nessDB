#include "btree.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h> /* htonl/ntohl */


#define FREE_QUEUE_LEN	64

struct chunk 
{
	size_t offset;
	size_t len;
};

static struct chunk free_queue[FREE_QUEUE_LEN];
static size_t free_queue_len = 0;

static inline __be32 to_be32(uint32_t x)
{
	return (FORCE __be32) htonl(x);
}

static inline __be16 to_be16(uint16_t x)
{
	return (FORCE __be16) htons(x);
}

static inline uint32_t from_be32(__be32 x)
{
	return ntohl((FORCE uint32_t) x);
}

static inline uint16_t from_be16(__be16 x)
{
	return ntohs((FORCE uint16_t) x);
}

static inline uint64_t ntoh64(uint64_t x)
{
#if (BYTE_ORDER == LITTLE_ENDIAN)
	return ((uint64_t) ntohl((uint32_t) x) << 32) |
		ntohl((uint32_t) (x >> 32));
#else
	return x;
#endif
}

static struct btree_table *alloc_table()
{
	struct btree_table *table = malloc(sizeof *table);
	memset(table, 0, sizeof *table);
	return table;
}

static struct btree_table *get_table(struct btree *btree, size_t offset)
{
	assert(offset != 0);

	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
	if (slot->offset == offset) {
		slot->offset = 0;
		return slot->table;
	}

	struct btree_table *table = malloc(sizeof *table);

	lseek(btree->fd, offset, SEEK_SET);
	if (read(btree->fd, table, sizeof *table) != sizeof *table)
		assert(0);
	return table;
}

/* Free a table acquired with 'alloc_table' or 'get_table' */
static void put_table(struct btree *btree, struct btree_table *table,
		      size_t offset)
{
	assert(offset != 0);

	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
	if (slot->offset)
		free(slot->table);
	slot->offset = offset;
	slot->table = table;
}

/* Write a table and free it */
static void flush_table(struct btree *btree, struct btree_table *table,
			size_t offset)
{
	assert(offset != 0);

	lseek(btree->fd, offset, SEEK_SET);
	if (write(btree->fd, table, sizeof *table) != sizeof *table)
		assert(0);

	put_table(btree, table, offset);
}

/* Open an existing database file */
int btree_open(struct btree *btree,const char* dbname)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open(dbname, O_RDWR);
	if (btree->fd < 0)
		return -1;

	struct btree_super super;
	if (read(btree->fd, &super, sizeof super) != sizeof super)
		return -1;
	btree->top = from_be32(super.top);
	btree->free_top = from_be32(super.free_top);

	btree->alloc = lseek(btree->fd, 0, SEEK_END);
	return 0;
}

static void flush_super(struct btree *btree);

/* Create and initialize a new database file */
int btree_creat(struct btree *btree,const char* dbname)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open(dbname, O_RDWR | O_TRUNC | O_CREAT, 0644);
	if (btree->fd < 0)
		return -1;

	flush_super(btree);

	btree->alloc = lseek(btree->fd, 0, SEEK_END);
	return 0;
}

/* Close a database file opened with 'btree_creat' or 'btree_open' */
void btree_close(struct btree *btree)
{
	close(btree->fd);

	size_t i;
	for (i = 0; i < CACHE_SLOTS; ++i) {
		if (btree->cache[i].offset)
			free(btree->cache[i].table);
	}
}

static int in_allocator = 0;

static size_t delete_table(struct btree *btree, size_t table_offset,
			   uint8_t *sha1);

static size_t collapse(struct btree *btree, size_t table_offset);

/* Return a value that is greater or equal to 'val' and is power-of-two. */
static size_t round_power2(size_t val)
{
	size_t i = 1;
	while (i < val)
		i <<= 1;
	return i;
}

static void free_chunk(struct btree *btree, size_t offset, size_t len);

/* Allocate a chunk from the database file */
static size_t alloc_chunk(struct btree *btree, size_t len)
{

	len = round_power2(len);

	size_t offset = 0;
	if (!in_allocator) {
		/* find free chunk with the larger or the same size */
		uint8_t sha1[SHA1_LENGTH];
		memset(sha1, 0, sizeof sha1);
		*(__be32 *)sha1 = to_be32(len);

		in_allocator = 1;
		offset = delete_table(btree, btree->free_top, sha1);
		if (offset) {
			btree->free_top = collapse(btree, btree->free_top);
			in_allocator = 0;

			size_t free_len = from_be32(*(__be32 *)sha1);
			assert(free_len >= len);
			assert(round_power2(free_len) == free_len);

			/* free extra space at the end of the chunk */
			while (free_len > len) {
				free_len >>= 1;
				free_chunk(btree, offset + free_len,
						  free_len);
			}
		} else
			in_allocator = 0;
	}
	if (offset == 0) {
		/* not found, allocate from the end of the file */
		offset = btree->alloc;
		btree->alloc += len;
	}
	return offset;
}

size_t insert_toplevel(struct btree *btree, size_t *table_offset,
		uint8_t *sha1, const void *data, size_t len);

/* Mark a chunk as unused in the database file */
static void free_chunk(struct btree *btree, size_t offset, size_t len)
{
	assert(len > 0);
	assert(offset != 0);

	len = round_power2(len);

	if (in_allocator) {
		if (free_queue_len >= FREE_QUEUE_LEN) {
			fprintf(stderr, "btree: free queue overflow\n");
			return;
		}
		struct chunk *chunk = &free_queue[free_queue_len++];
		chunk->offset = offset;
		chunk->len = len;
		return;
	}

	uint8_t sha1[SHA1_LENGTH];
	memset(sha1, 0, sizeof sha1);
	*(__be32 *)sha1 = to_be32(len);
	((uint32_t *)sha1)[1] = rand();
	((uint32_t *)sha1)[2] = rand();

	in_allocator = 1;
	insert_toplevel(btree, &btree->free_top, sha1, NULL, offset);
	in_allocator = 0;
}

static void flush_super(struct btree *btree)
{
	/* free queued chunks */
	size_t i;
	for (i = 0; i < free_queue_len; ++i) {
		struct chunk *chunk = &free_queue[i];
		free_chunk(btree, chunk->offset, chunk->len);
	}
	free_queue_len = 0;

	struct btree_super super;
	memset(&super, 0, sizeof super);
	super.top = to_be32(btree->top);
	super.free_top = to_be32(btree->free_top);

	lseek(btree->fd, 0, SEEK_SET);
	if (write(btree->fd, &super, sizeof super) != sizeof super)
		assert(0);
}

static size_t insert_data(struct btree *btree, const void *data, size_t len)
{
	if (data == NULL)
		return len;

	struct blob_info info;
	memset(&info, 0, sizeof info);
	info.len = to_be32(len);

	size_t offset =alloc_chunk(btree,sizeof info + len);

	lseek(btree->fd, offset, SEEK_SET);
	if (write(btree->fd, &info, sizeof info) != sizeof info)
		assert(0);
	if (write(btree->fd, data, len) != len)
		assert(0);

	return offset;
}

/* Split a table. The pivot item is stored to 'sha1' and 'offset'.
   Returns offset to the new table. */
static size_t split_table(struct btree *btree, struct btree_table *table,
			  uint8_t *sha1, size_t *offset)
{
	memcpy(sha1, table->items[TABLE_SIZE / 2].sha1, SHA1_LENGTH);
	*offset = from_be32(table->items[TABLE_SIZE / 2].offset);

	struct btree_table *new_table = alloc_table(btree);
	new_table->size = table->size - TABLE_SIZE / 2 - 1;

	table->size = TABLE_SIZE / 2;

	memcpy(new_table->items, &table->items[TABLE_SIZE / 2 + 1],
		(new_table->size + 1) * sizeof(struct btree_item));

	size_t new_table_offset =alloc_chunk(btree, sizeof *new_table);
	flush_table(btree, new_table, new_table_offset);

	return new_table_offset;
}

/* Try to collapse the given table. Returns a new table offset. */
static size_t collapse(struct btree *btree, size_t table_offset)
{
	struct btree_table *table = get_table(btree, table_offset);
	if (table->size == 0) {
		size_t ret = from_be32(table->items[0].child);
		free_chunk(btree, table_offset, sizeof *table);
		put_table(btree, table, table_offset);
		return ret;
	}
	put_table(btree, table, table_offset);
	return table_offset;
}

static size_t remove_table(struct btree *btree, struct btree_table *table,
			   size_t i, uint8_t *sha1);

/* Find and remove the smallest item from the given table. The key of the item
   is stored to 'sha1'. Returns offset to the item */
static size_t take_smallest(struct btree *btree, size_t table_offset,
			      uint8_t *sha1)
{
	struct btree_table *table = get_table(btree, table_offset);
	assert(table->size > 0);

	size_t offset = 0;
	size_t child = from_be32(table->items[0].child);
	if (child == 0) {
		offset = remove_table(btree, table, 0, sha1);
	} else {
		offset = take_smallest(btree, child, sha1);
		table->items[0].child = to_be32(collapse(btree, child));
	}
	flush_table(btree, table, table_offset);
	return offset;
}

/* Find and remove the largest item from the given table. The key of the item
   is stored to 'sha1'. Returns offset to the item */
static size_t take_largest(struct btree *btree, size_t table_offset,
			     uint8_t *sha1)
{
	struct btree_table *table = get_table(btree, table_offset);
	assert(table->size > 0);

	size_t offset = 0;
	size_t child = from_be32(table->items[table->size].child);
	if (child == 0) {
		offset = remove_table(btree, table, table->size - 1, sha1);
	} else {
		offset = take_largest(btree, child, sha1);
		table->items[table->size].child =
			to_be32(collapse(btree, child));
	}
	flush_table(btree, table, table_offset);
	return offset;
}

/* Remove an item in position 'i' from the given table. The key of the
   removed item is stored to 'sha1'. Returns offset to the item. */
static size_t remove_table(struct btree *btree, struct btree_table *table,
			     size_t i, uint8_t *sha1)
{
	assert(i < table->size);

	if (sha1)
		memcpy(sha1, table->items[i].sha1, SHA1_LENGTH);

	size_t offset = from_be32(table->items[i].offset);
	size_t left_child = from_be32(table->items[i].child);
	size_t right_child = from_be32(table->items[i + 1].child);

	if (left_child && right_child) {
	        /* replace the removed item by taking an item from one of the
	           child tables */
		size_t new_offset;
		if (rand() & 1) {
			new_offset = take_largest(btree, left_child,
						  table->items[i].sha1);
			table->items[i].child =
				to_be32(collapse(btree, left_child));
		} else {
			new_offset = take_smallest(btree, right_child,
						   table->items[i].sha1);
			table->items[i + 1].child =
				to_be32(collapse(btree, right_child));
		}
		table->items[i].offset = to_be32(new_offset);

	} else {
		memmove(&table->items[i], &table->items[i + 1],
			(table->size - i) * sizeof(struct btree_item));
		table->size--;

		if (left_child)
			table->items[i].child = to_be32(left_child);
		else
			table->items[i].child = to_be32(right_child);
	}
	return offset;
}

/* Insert a new item with key 'sha1' with the contents in 'data' to the given
   table. Returns offset to the new item. */
static size_t insert_table(struct btree *btree, size_t table_offset,
			 uint8_t *sha1, const void *data, size_t len)
{
	struct btree_table *table = get_table(btree, table_offset);
	assert(table->size < TABLE_SIZE-1);

	size_t left = 0, right = table->size;
	while (left < right) {
		size_t i = (left + right) / 2;
		int cmp = strcmp((const char*)sha1, (const char*)table->items[i].sha1);
		if (cmp == 0) {
			/* already in the table */
			size_t ret = from_be32(table->items[i].offset);
			put_table(btree, table, table_offset);
			return ret;
		}
		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	size_t i = left;

	size_t offset = 0;
	size_t child_offset = from_be32(table->items[i].child);
	size_t right_child = 0;
	size_t ret = 0;
	if (child_offset) {
		/* recursion */
		ret = insert_table(btree, child_offset, sha1, data, len);

		/* check if we need to split */
		struct btree_table *child = get_table(btree, child_offset);
		if (child->size < TABLE_SIZE-1) {
			put_table(btree, table, table_offset);
			put_table(btree, child, child_offset);
			return ret;
		}
		right_child = split_table(btree, child, sha1, &offset);
		flush_table(btree, child, child_offset);
	} else {
		ret = offset = insert_data(btree, data, len);
	}

	table->size++;
	memmove(&table->items[i + 1], &table->items[i],
		(table->size - i) * sizeof(struct btree_item));
	memcpy(table->items[i].sha1, sha1, SHA1_LENGTH);
	table->items[i].offset = to_be32(offset);
	table->items[i].child = to_be32(child_offset);
	table->items[i + 1].child = to_be32(right_child);
	flush_table(btree, table, table_offset);
	return ret;
}

static void dump_sha1(const uint8_t *sha1)
{
	size_t i;
	for (i = 0; i < SHA1_LENGTH; i++)
		printf("%02x", sha1[i]);
}

/* Remove a item with key 'sha1' from the given table. The offset to the
   removed item is returned. */
static size_t delete_table(struct btree *btree, size_t table_offset,
			   uint8_t *sha1)
{
	if (table_offset == 0)
		return 0;
	struct btree_table *table = get_table(btree, table_offset);

	size_t left = 0, right = table->size;
	while (left < right) {
		size_t i = (left + right) / 2;
		int cmp = strcmp((const char*)sha1, (const char*)table->items[i].sha1);
		if (cmp == 0) {
			/* found */
			size_t ret = remove_table(btree, table, i, sha1);
			flush_table(btree, table, table_offset);
			return ret;
		}
		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}

	/* not found - recursion */
	size_t i = left;
	size_t ret = 0;
	size_t child = from_be32(table->items[i].child);
	ret = delete_table(btree, child, sha1);
	if (ret)
		table->items[i].child = to_be32(collapse(btree, child));

	if (ret == 0 && in_allocator && i < table->size) {
		/* remove the next largest */
		ret = remove_table(btree, table, i, sha1);
	}
	if (ret)
		flush_table(btree, table, table_offset);
	else
		put_table(btree, table, table_offset);
	return ret;
}

size_t insert_toplevel(struct btree *btree, size_t *table_offset,
			uint8_t *sha1, const void *data, size_t len)
{
	size_t offset = 0;
	size_t ret = 0;
	size_t right_child = 0;
	if (*table_offset) {
		ret = insert_table(btree, *table_offset, sha1, data, len);

		/* check if we need to split */
		struct btree_table *table = get_table(btree, *table_offset);
		if (table->size < TABLE_SIZE-1) {
			put_table(btree, table, *table_offset);
			return ret;
		}
		right_child = split_table(btree, table, sha1, &offset);
		flush_table(btree, table, *table_offset);
	} else {
		ret = offset = insert_data(btree, data, len);
	}

	struct btree_table *new_table = alloc_table(btree);
	new_table->size = 1;
	memcpy(new_table->items[0].sha1, sha1, SHA1_LENGTH);
	new_table->items[0].offset = to_be32(offset);
	new_table->items[0].child = to_be32(*table_offset);
	new_table->items[1].child = to_be32(right_child);

	size_t new_table_offset =alloc_chunk(btree, sizeof *new_table);
	flush_table(btree, new_table, new_table_offset);

	*table_offset = new_table_offset;
	return ret;
}

/* Insert a new item with key 'sha1' with the contents in 'data' to the
   database file. */
size_t btree_insert(struct btree *btree, const uint8_t *c_sha1, const void *data,size_t len)
{
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1, c_sha1, sizeof sha1);
	size_t ret=insert_toplevel(btree, &btree->top, sha1, data, len);
	flush_super(btree);
	return ret;
}

/* Look up item with the given key 'sha1' in the given table. Returns offset
   to the item. */
static size_t lookup(struct btree *btree, size_t table_offset,
		     const uint8_t *sha1)
{
	while (table_offset) {
		struct btree_table *table = get_table(btree, table_offset);
		size_t left = 0, right = table->size, i;
		while (left < right) {
			i = (left + right) / 2;
			int cmp = strcmp((const char*)sha1, (const char*)table->items[i].sha1);
			if (cmp == 0) {
				/* found */
				size_t ret = from_be32(table->items[i].offset);
				put_table(btree, table, table_offset);
				return ret;
			}
			if (cmp < 0)
				right = i;
			else
				left = i + 1;
		}
		size_t child = from_be32(table->items[left].child);
		put_table(btree, table, table_offset);
		table_offset = child;
	}
	return 0;
}

/* Look up item with the given key 'sha1' in the database file. Length of the
   item is stored in 'len'. Returns a pointer to the contents of the item. */
void *btree_get(struct btree *btree, const uint8_t *sha1)
{
	size_t offset = lookup(btree, btree->top, sha1);
	if (offset == 0)
		return NULL;

	lseek(btree->fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->fd, &info, sizeof info) != sizeof info)
		return NULL;
	size_t len = from_be32(info.len);

	void *data = malloc(len);
	if (data == NULL)
		return NULL;
	if (read(btree->fd, data, len) != len) {
		free(data);
		data = NULL;
	}
	return data;
}

void *btree_get_value(struct btree *btree,size_t offset)
{
	if (offset == 0)
		return NULL;

	lseek(btree->fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->fd, &info, sizeof info) != sizeof info)
		return NULL;
	size_t len = from_be32(info.len);

	void *data = malloc(len);
	if (data == NULL)
		return NULL;
	if (read(btree->fd, data, len) != len) {
		free(data);
		data = NULL;
	}
	return data;
}

/* Remove item with the given key 'sha1' from the database file */
int btree_delete(struct btree *btree, const uint8_t *c_sha1)
{
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1, c_sha1, sizeof sha1);
	size_t offset = delete_table(btree, btree->top, sha1);
	if (offset == 0)
		return -1;

	btree->top = collapse(btree, btree->top);
	flush_super(btree);

	lseek(btree->fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->fd, &info, sizeof info) != sizeof info)
		return 0;

	free_chunk(btree, offset, sizeof info + from_be32(info.len));
	flush_super(btree);
	return 0;
}
