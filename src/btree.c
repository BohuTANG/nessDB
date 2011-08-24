#include "btree.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef _WIN32
#include <winsock.h>
#else
/* Unix */
#include <arpa/inet.h> /* htonl/ntohl */
#define O_BINARY 0
#endif

#define FREE_QUEUE_LEN	64

struct chunk {
	uint64_t offset;
	uint64_t len;
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

static inline __be64 to_be64(uint64_t x)
{
#if (BYTE_ORDER == LITTLE_ENDIAN)
	return (FORCE __be64) (((uint64_t) htonl((uint32_t) x) << 32) |
			       htonl((uint32_t) (x >> 32)));
#else
	return (FORCE __be64) x;
#endif
}

static inline uint32_t from_be32(__be32 x)
{
	return ntohl((FORCE uint32_t) x);
}

static inline uint16_t from_be16(__be16 x)
{
	return ntohs((FORCE uint16_t) x);
}

static inline uint64_t from_be64(__be64 x)
{
#if (BYTE_ORDER == LITTLE_ENDIAN)
	return ((uint64_t) ntohl((uint32_t) (FORCE uint64_t) x) << 32) |
		ntohl((uint32_t) ((FORCE uint64_t) x >> 32));
#else
	return (FORCE uint64_t) x;
#endif
}

/* 15 times faster than gcc's memcmp on x86-64 */
static int cmp_sha1(const uint8_t *a, const uint8_t *b)
{
	return strcmp(a,b);
}

static struct btree_table *alloc_table(struct btree *btree)
{
	struct btree_table *table = malloc(sizeof *table);
	memset(table, 0, sizeof *table);
	return table;
}

static struct btree_table *get_table(struct btree *btree, uint64_t offset)
{
	assert(offset != 0);

	/* take from cache */
	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
	if (slot->offset == offset) {
		slot->offset = 0;
		return slot->table;
	}

	struct btree_table *table = malloc(sizeof *table);

	lseek64(btree->fd, offset, SEEK_SET);
	if (read(btree->fd, table, sizeof *table) != (ssize_t) sizeof *table) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
	return table;
}

/* Free a table acquired with alloc_table() or get_table() */
static void put_table(struct btree *btree, struct btree_table *table,
		      uint64_t offset)
{
	assert(offset != 0);

	/* overwrite cache */
	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
	if (slot->offset != 0) {
		free(slot->table);
	}
	slot->offset = offset;
	slot->table = table;
}

/* Write a table and free it */
static void flush_table(struct btree *btree, struct btree_table *table,
			uint64_t offset)
{
	assert(offset != 0);

	lseek64(btree->fd, offset, SEEK_SET);
	if (write(btree->fd, table, sizeof *table) != (ssize_t) sizeof *table) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
	put_table(btree, table, offset);
}

int btree_open(struct btree *btree, const char *fname)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open64(fname, O_RDWR | O_BINARY);
	if (btree->fd < 0)
		return -1;

	struct btree_super super;
	if (read(btree->fd, &super, sizeof super) != (ssize_t) sizeof super)
		return -1;
	btree->top = from_be64(super.top);
	btree->free_top = from_be64(super.free_top);

	btree->alloc =lseek64(btree->fd, 0, SEEK_END);
	return 0;
}

static void flush_super(struct btree *btree);

int btree_creat(struct btree *btree, const char *fname)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open64(fname, O_RDWR | O_TRUNC | O_CREAT | O_BINARY, 0644);
	if (btree->fd < 0)
		return -1;

	flush_super(btree);

	btree->alloc =sizeof(struct btree_super);
	lseek64(btree->fd, 0, SEEK_END);
	return 0;
}

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
static int delete_larger = 0;

static uint64_t delete_table(struct btree *btree, uint64_t table_offset,
			   uint8_t *sha1);

static uint64_t collapse(struct btree *btree, uint64_t table_offset);

/* Return a value that is greater or equal to 'val' and is power-of-two. */
static size_t round_power2(size_t val)
{
	size_t i = 1;
	while (i < val)
		i <<= 1;
	return i;
}

static void free_chunk(struct btree *btree, uint64_t offset, size_t len);

/* Allocate a chunk from the database file */
static uint64_t alloc_chunk(struct btree *btree, size_t len)
{
	assert(len > 0);

	len = round_power2(len);

	uint64_t offset = 0;
	if (!in_allocator) {
		/* create fake size SHA-1 */
		uint8_t sha1[SHA1_LENGTH];
		memset(sha1, 0, sizeof sha1);
		*(uint32_t *) sha1 = -1;
		((__be32 *) sha1)[1] = to_be32(len);

		/* find free chunk with the larger or the same size/SHA-1 */
		in_allocator = 1;
		delete_larger = 1;
		offset = delete_table(btree, btree->free_top, sha1);
		delete_larger = 0;
		if (offset) {
			assert(*(uint32_t *) sha1 == (uint32_t) -1);
			size_t free_len = from_be32(((__be32 *) sha1)[1]);
			assert(round_power2(free_len) == free_len);
			assert(free_len >= len);

			/* delete buddy information */
			memset(sha1, 0, sizeof sha1);
			*(__be64 *) sha1 = to_be64(offset);
			uint64_t buddy_len =
				delete_table(btree, btree->free_top, sha1);
			assert(buddy_len == len);

			btree->free_top = collapse(btree, btree->free_top);

			in_allocator = 0;

			/* free extra space at the end of the chunk */
			while (free_len > len) {
				free_len >>= 1;
				free_chunk(btree, offset + free_len,
						  free_len);
			}
		} else {
			in_allocator = 0;
		}
	}
	if (offset == 0) {
		/* not found, allocate from the end of the file */
		offset = btree->alloc;
		/* TODO: this wastes memory.. */
		if (offset & (len - 1)) {
			offset += len - (offset & (len - 1));
		}
		btree->alloc = offset + len;
	}
	return offset;
}

static uint64_t lookup(struct btree *btree, uint64_t table_offset,
		    const uint8_t *sha1);
uint64_t insert_toplevel(struct btree *btree, uint64_t *table_offset,
		      uint8_t *sha1, const void *data, size_t len);

/* Mark a chunk as unused in the database file */
static void free_chunk(struct btree *btree, uint64_t offset, size_t len)
{
	assert(len > 0);
	assert(offset != 0);
	len = round_power2(len);
	assert((offset & (len - 1)) == 0);

	if (in_allocator) {
		/* add to queue to avoid entering the allocator again */
		if (free_queue_len >= FREE_QUEUE_LEN) {
			fprintf(stderr, "btree: free queue overflow\n");
			return;
		}
		struct chunk *chunk = &free_queue[free_queue_len++];
		chunk->offset = offset;
		chunk->len = len;
		return;
	}

	/* create fake offset SHA-1 for buddy allocation */
	uint8_t sha1[SHA1_LENGTH];
#if 0
	memset(sha1, 0, sizeof sha1);
	*(__be64 *) sha1 = to_be64(offset ^ len);
	uint64_t buddy_len = lookup(btree, btree->free_top, sha1);
	if (buddy_len != 0) {
		assert(len == buddy_len);
		/* TODO: combine with buddy (recursively!) */
	}
#endif

	in_allocator = 1;

	/* add buddy information */
	memset(sha1, 0, sizeof sha1);
	*(__be64 *) sha1 = to_be64(offset);
	insert_toplevel(btree, &btree->free_top, sha1, NULL, len);

	/* add allocation information */
	memset(sha1, 0, sizeof sha1);
	*(uint32_t *) sha1 = -1;
	((__be32 *) sha1)[1] = to_be32(len);
	((uint32_t *) sha1)[2] = rand(); /* to make SHA-1 unique */
	((uint32_t *) sha1)[3] = rand();
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
	super.top = to_be64(btree->top);
	super.free_top = to_be64(btree->free_top);

	lseek64(btree->fd, 0, SEEK_SET);
	if (write(btree->fd, &super, sizeof super) != sizeof super) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
}

static uint64_t insert_data(struct btree *btree, const void *data, size_t len)
{
	if (data == NULL)
		return len;

	struct blob_info info;
	memset(&info, 0, sizeof info);
	info.len = to_be32(len);

	uint64_t offset = alloc_chunk(btree, sizeof info + len);

	lseek64(btree->fd, offset, SEEK_SET);
	if (write(btree->fd, &info, sizeof info) != sizeof info) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
	if (write(btree->fd, data, len) != (ssize_t) len) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}

	return offset;
}

/* Split a table. The pivot item is stored to 'sha1' and 'offset'.
   Returns offset to the new table. */
static uint64_t split_table(struct btree *btree, struct btree_table *table,
			  uint8_t *sha1, uint64_t *offset)
{
	memcpy(sha1, table->items[TABLE_SIZE / 2].sha1, SHA1_LENGTH);
	*offset = from_be64(table->items[TABLE_SIZE / 2].offset);

	struct btree_table *new_table = alloc_table(btree);
	new_table->size = table->size - TABLE_SIZE / 2 - 1;

	table->size = TABLE_SIZE / 2;

	memcpy(new_table->items, &table->items[TABLE_SIZE / 2 + 1],
		(new_table->size + 1) * sizeof(struct btree_item));

	uint64_t new_table_offset = alloc_chunk(btree, sizeof *new_table);
	flush_table(btree, new_table, new_table_offset);

	return new_table_offset;
}

/* Try to collapse the given table. Returns a new table offset. */
static uint64_t collapse(struct btree *btree, uint64_t table_offset)
{
	struct btree_table *table = get_table(btree, table_offset);
	if (table->size == 0) {
		uint64_t ret = from_be64(table->items[0].child);
		free_chunk(btree, table_offset, sizeof *table);
		put_table(btree, table, table_offset);
		return ret;
	}
	put_table(btree, table, table_offset);
	return table_offset;
}

static uint64_t remove_table(struct btree *btree, struct btree_table *table,
			   size_t i, uint8_t *sha1);

/* Find and remove the smallest item from the given table. The key of the item
   is stored to 'sha1'. Returns offset to the item */
static uint64_t take_smallest(struct btree *btree, uint64_t table_offset,
			      uint8_t *sha1)
{
	struct btree_table *table = get_table(btree, table_offset);
	assert(table->size > 0);

	uint64_t offset = 0;
	uint64_t child = from_be64(table->items[0].child);
	if (child == 0) {
		offset = remove_table(btree, table, 0, sha1);
	} else {
		/* recursion */
		offset = take_smallest(btree, child, sha1);
		table->items[0].child = to_be64(collapse(btree, child));
	}
	flush_table(btree, table, table_offset);
	return offset;
}

/* Find and remove the largest item from the given table. The key of the item
   is stored to 'sha1'. Returns offset to the item */
static uint64_t take_largest(struct btree *btree, uint64_t table_offset,
			     uint8_t *sha1)
{
	struct btree_table *table = get_table(btree, table_offset);
	assert(table->size > 0);

	uint64_t offset = 0;
	uint64_t child = from_be64(table->items[table->size].child);
	if (child == 0) {
		offset = remove_table(btree, table, table->size - 1, sha1);
	} else {
		/* recursion */
		offset = take_largest(btree, child, sha1);
		table->items[table->size].child =
			to_be64(collapse(btree, child));
	}
	flush_table(btree, table, table_offset);
	return offset;
}

/* Remove an item in position 'i' from the given table. The key of the
   removed item is stored to 'sha1'. Returns offset to the item. */
static uint64_t remove_table(struct btree *btree, struct btree_table *table,
			     size_t i, uint8_t *sha1)
{
	assert(i < table->size);

	if (sha1)
		memcpy(sha1, table->items[i].sha1, SHA1_LENGTH);

	uint64_t offset = from_be64(table->items[i].offset);
	uint64_t left_child = from_be64(table->items[i].child);
	uint64_t right_child = from_be64(table->items[i + 1].child);

	if (left_child != 0 && right_child != 0) {
	        /* replace the removed item by taking an item from one of the
	           child tables */
		uint64_t new_offset;
		if (rand() & 1) {
			new_offset = take_largest(btree, left_child,
						  table->items[i].sha1);
			table->items[i].child =
				to_be64(collapse(btree, left_child));
		} else {
			new_offset = take_smallest(btree, right_child,
						   table->items[i].sha1);
			table->items[i + 1].child =
				to_be64(collapse(btree, right_child));
		}
		table->items[i].offset = to_be64(new_offset);

	} else {
		memmove(&table->items[i], &table->items[i + 1],
			(table->size - i) * sizeof(struct btree_item));
		table->size--;

		if (left_child != 0)
			table->items[i].child = to_be64(left_child);
		else
			table->items[i].child = to_be64(right_child);
	}
	return offset;
}

/* Insert a new item with key 'sha1' with the contents in 'data' to the given
   table. Returns offset to the new item. */
static uint64_t insert_table(struct btree *btree, uint64_t table_offset,
			 uint8_t *sha1, const void *data, size_t len)
{
	struct btree_table *table = get_table(btree, table_offset);
	assert(table->size < TABLE_SIZE-1);

	size_t left = 0, right = table->size;
	while (left < right) {
		size_t i = (right - left) / 2 + left;
		int cmp = cmp_sha1(sha1, table->items[i].sha1);
		if (cmp == 0) {
			/* already in the table */
			uint64_t ret = from_be64(table->items[i].offset);
			put_table(btree, table, table_offset);
			return ret;
		}
		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	size_t i = left;

	uint64_t offset = 0;
	uint64_t left_child = from_be64(table->items[i].child);
	uint64_t right_child = 0; /* after insertion */
	uint64_t ret = 0;
	if (left_child != 0) {
		/* recursion */
		ret = insert_table(btree, left_child, sha1, data, len);

		/* check if we need to split */
		struct btree_table *child = get_table(btree, left_child);
		if (child->size < TABLE_SIZE-1) {
			/* nothing to do */
			put_table(btree, table, table_offset);
			put_table(btree, child, left_child);
			return ret;
		}
		/* overwrites SHA-1 */
		right_child = split_table(btree, child, sha1, &offset);
		/* flush just in case changes happened */
		flush_table(btree, child, left_child);
	} else {
		ret = offset = insert_data(btree, data, len);
	}

	table->size++;
	memmove(&table->items[i + 1], &table->items[i],
		(table->size - i) * sizeof(struct btree_item));
	memcpy(table->items[i].sha1, sha1, SHA1_LENGTH);
	table->items[i].offset = to_be64(offset);
	table->items[i].child = to_be64(left_child);
	table->items[i + 1].child = to_be64(right_child);

	flush_table(btree, table, table_offset);
	return ret;
}

#if 0
static void dump_sha1(const uint8_t *sha1)
{
	size_t i;
	for (i = 0; i < SHA1_LENGTH; i++)
		printf("%02x", sha1[i]);
}
#endif

/*
 * Remove a item with key 'sha1' from the given table. The offset to the
 * removed item is returned.
 * Please note that 'sha1' is overwritten when called inside the allocator.
 */
static uint64_t delete_table(struct btree *btree, uint64_t table_offset,
			   uint8_t *sha1)
{
	if (table_offset == 0)
		return 0;
	struct btree_table *table = get_table(btree, table_offset);

	size_t left = 0, right = table->size;
	while (left < right) {
		size_t i = (right - left) / 2 + left;
		int cmp = cmp_sha1(sha1, table->items[i].sha1);
		if (cmp == 0) {
			/* found */
			uint64_t ret = remove_table(btree, table, i, sha1);
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
	uint64_t child = from_be64(table->items[i].child);
	uint64_t ret = delete_table(btree, child, sha1);
	if (ret != 0) {
		table->items[i].child = to_be64(collapse(btree, child));
	}

	if (ret == 0 && delete_larger && i < table->size) {
		/* remove the next largest */
		ret = remove_table(btree, table, i, sha1);
	}
	if (ret != 0) {
		/* flush just in case changes happened */
		flush_table(btree, table, table_offset);
	} else {
		put_table(btree, table, table_offset);
	}
	return ret;
}

uint64_t insert_toplevel(struct btree *btree, uint64_t *table_offset,
			uint8_t *sha1, const void *data, size_t len)
{
	uint64_t offset = 0;
	uint64_t ret = 0;
	uint64_t right_child = 0;
	if (*table_offset != 0) {
		ret = insert_table(btree, *table_offset, sha1, data, len);

		/* check if we need to split */
		struct btree_table *table = get_table(btree, *table_offset);
		if (table->size < TABLE_SIZE-1) {
			/* nothing to do */
			put_table(btree, table, *table_offset);
			return ret;
		}
		right_child = split_table(btree, table, sha1, &offset);
		flush_table(btree, table, *table_offset);
	} else {
		ret = offset = insert_data(btree, data, len);
	}

	/* create new top level table */
	struct btree_table *new_table = alloc_table(btree);
	new_table->size = 1;
	memcpy(new_table->items[0].sha1, sha1, SHA1_LENGTH);
	new_table->items[0].offset = to_be64(offset);
	new_table->items[0].child = to_be64(*table_offset);
	new_table->items[1].child = to_be64(right_child);

	uint64_t new_table_offset = alloc_chunk(btree, sizeof *new_table);
	flush_table(btree, new_table, new_table_offset);

	*table_offset = new_table_offset;
	return ret;
}

void btree_insert(struct btree *btree, const uint8_t *c_sha1, const void *data,
		  size_t len)
{
	/* SHA-1 must be in writable memory */
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1, c_sha1, sizeof sha1);

	insert_toplevel(btree, &btree->top, sha1, data, len);
	flush_super(btree);
}

/*
 * Look up item with the given key 'sha1' in the given table. Returns offset
 * to the item.
 */
static uint64_t lookup(struct btree *btree, uint64_t table_offset,
		    const uint8_t *sha1)
{
	while (table_offset) {
		struct btree_table *table = get_table(btree, table_offset);
		size_t left = 0, right = table->size, i;
		while (left < right) {
			i = (right - left) / 2 + left;
			int cmp = cmp_sha1(sha1, table->items[i].sha1);
			if (cmp == 0) {
				/* found */
				uint64_t ret = from_be64(table->items[i].offset);
				put_table(btree, table, table_offset);
				return ret;
			}
			if (cmp < 0)
				right = i;
			else
				left = i + 1;
		}
		uint64_t  child = from_be64(table->items[left].child);
		put_table(btree, table, table_offset);
		table_offset = child;
	}
	return 0;
}

void *btree_get(struct btree *btree, const uint8_t *sha1, size_t *len,uint64_t *val_offset)
{
	uint64_t offset = lookup(btree, btree->top, sha1);
	if (offset == 0)
		return NULL;

	*val_offset=offset;
	lseek64(btree->fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;
	*len = from_be32(info.len);

	void *data = malloc(*len);
	if (data == NULL)
		return NULL;
	if (read(btree->fd, data, *len) != (ssize_t) *len) {
		free(data);
		data = NULL;
	}
	return data;
}

void *btree_get_byoffset(struct btree *btree,uint64_t offset,size_t *len)
{
	if (offset == 0)
		return NULL;

	lseek64(btree->fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;
	*len = from_be32(info.len);

	void *data = malloc(*len);
	if (data == NULL)
		return NULL;
	if (read(btree->fd, data, *len) != (ssize_t) *len) {
		free(data);
		data = NULL;
	}
	return data;
}

int btree_delete(struct btree *btree, const uint8_t *c_sha1)
{
	/* SHA-1 must be in writable memory */
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1, c_sha1, sizeof sha1);

	uint64_t offset = delete_table(btree, btree->top, sha1);
	if (offset == 0)
		return -1;

	btree->top = collapse(btree, btree->top);
	flush_super(btree);

	lseek64(btree->fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->fd, &info, sizeof info) != sizeof info)
		return 0;

	free_chunk(btree, offset, sizeof info + from_be32(info.len));
	flush_super(btree);
	return 0;
}

