 /*Based on Japeq's btree(https://github.com/japeq/btree_db)
 *
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif

#ifndef __USE_LARGEFILE64
#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include "storage.h"

#define IDXEXT	".idx"
#define DBEXT	".db"

struct chunk {
	uint64_t offset;
	uint64_t len;
};

static int cmp_sha1(const uint8_t *a, const uint8_t *b)
{
	return strcmp((const char*)a,(const char*)b);
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

static void flush_super(struct btree *btree)
{

	struct btree_super super;
	memset(&super, 0, sizeof super);
	super.top = to_be64(btree->top);
	super.free_top = to_be64(btree->free_top);


	lseek64(btree->fd, 0, SEEK_SET);
	if (write(btree->fd, &super, sizeof super) != sizeof super){
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
}
static void flush_magic(struct btree *btree)
{
	int magic=2011;
	if (write(btree->db_fd, &magic, sizeof magic) != sizeof magic){
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
}



static uint64_t getsize(int fd) {
    struct stat64 sb;

    if (fstat64(fd,&sb) == -1) return 0;
    return (uint64_t) sb.st_size;
}

static int btree_open(struct btree *btree,const char *idx,const char *db)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open(idx, O_RDWR | O_BINARY | O_LARGEFILE);
	btree->db_fd = open(db, O_RDWR | O_BINARY | O_LARGEFILE);

	if (btree->fd < 0 || btree->db_fd<0)
		return -1;

	struct btree_super super;
	if (read(btree->fd, &super, sizeof super) != (ssize_t) sizeof super)
		return -1;
	btree->top = from_be64(super.top);
	btree->free_top = from_be64(super.free_top);

	btree->alloc =getsize(btree->fd);
	btree->db_alloc =getsize(btree->db_fd);

	return 0;
}


static int btree_creat(struct btree *btree,const char *idx,const char *db)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open(idx, O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE, 0644);
	btree->db_fd = open(db, O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE, 0644);

	if (btree->fd < 0 || btree->db_fd<0)
		return -1;

	flush_super(btree);

	btree->alloc =sizeof(struct btree_super);
	lseek64(btree->fd, 0, SEEK_END);

	flush_super(btree);
	btree->db_alloc=sizeof(int);
	return 0;
}

static int file_exists(const char *path)
{
	int fd=open64(path, O_RDWR);
	if(fd>-1){
		close(fd);
		return 1;
	}
	return 0;
}

int btree_init(struct btree *btree,const char *dbname,int isbgsync)
{
	char idx[256]={0};
	char db[256]={0};

	sprintf(idx,"%s%s",dbname,IDXEXT);
	sprintf(db,"%s%s",dbname,DBEXT);

	if(file_exists(idx))
		btree_open(btree,idx,db);
	else
		btree_creat(btree,idx,db);

}

void btree_close(struct btree *btree)
{
	close(btree->fd);
	close(btree->db_fd);

	size_t i;
	for (i = 0; i < CACHE_SLOTS; ++i) {
		if (btree->cache[i].offset)
			free(btree->cache[i].table);
	}
}


/* Allocate a chunk from the index file */
static uint64_t alloc_chunk(struct btree *btree, size_t len)
{
	uint64_t offset = btree->alloc;
	btree->alloc = offset + len;
	return offset;
}


/* Allocate a chunk from the database file */
static uint64_t alloc_db_chunk(struct btree *btree, size_t len)
{
	len = round_power2(len);

	uint64_t offset  = btree->db_alloc;
	btree->db_alloc = offset + len;
	return offset;
}



static uint64_t insert_data(struct btree *btree, const void *data, size_t len)
{
	if (data == NULL)
		return len;

	struct blob_info info;
	memset(&info, 0, sizeof info);
	info.len = to_be32(len);

	uint64_t offset = alloc_db_chunk(btree, sizeof info + len);

	lseek64(btree->db_fd, offset, SEEK_SET);
	if (write(btree->db_fd, &info, sizeof info) != sizeof info) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
	if (write(btree->db_fd, data, len) != (ssize_t) len) {
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


/* Insert a new item with key 'sha1' with the contents in 'data' to the given
   table. Returns offset to the new item. */
static uint64_t insert_table(struct btree *btree, uint64_t table_offset,
			 uint8_t *sha1, const void *data, size_t len,const uint64_t *v_off)
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
		ret = insert_table(btree, left_child, sha1, data, len,v_off);

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
		if(v_off)
			ret=offset=*v_off;
		else
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


/*
 * Remove a item with key 'sha1' from the given table. The offset to the
 * removed item is returned.
 * Please note that 'sha1' is overwritten when called inside the allocator.
 */
static uint64_t delete_table(struct btree *btree, uint64_t table_offset,
			   uint8_t *sha1)
{
	while (table_offset) {
		struct btree_table *table = get_table(btree, table_offset);
		size_t left = 0, right = table->size, i;
		while (left < right) {
			i = (right - left) / 2 + left;
			int cmp = cmp_sha1(sha1, table->items[i].sha1);
			if (cmp == 0) {
				/* found */
				//mark unused
				uint64_t off = from_be64(table->items[i].offset);
				table->items[i].offset=to_be64(set_H_1(off));
				flush_table(btree, table, table_offset);
				return 1;
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

uint64_t insert_toplevel(struct btree *btree, uint64_t *table_offset,
			uint8_t *sha1, const void *data, size_t len,const uint64_t *v_off)
{
	uint64_t offset = 0;
	uint64_t ret = 0;
	uint64_t right_child = 0;
	if (*table_offset != 0) {
		ret = insert_table(btree, *table_offset, sha1, data, len,v_off);

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
		if(v_off)
			ret=offset=*v_off;
		else
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

uint64_t btree_insert(struct btree *btree, const uint8_t *c_sha1, const void *data,
		  size_t len)
{
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1, c_sha1, sizeof sha1);

	uint64_t ret=insert_toplevel(btree, &btree->top, sha1, data, len,NULL);
	flush_super(btree);
	return ret;
}

uint64_t btree_insert_index(struct btree *btree,const uint8_t *c_sha1,const uint64_t *v_off)
{
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1,c_sha1,sizeof sha1);

	uint64_t ret=insert_toplevel(btree,&btree->top,sha1,NULL,0,v_off);
	return ret;
}

uint64_t btree_insert_data(struct btree *btree, const void *data,
		  size_t len)
{
	return insert_data(btree, data, len);
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
				uint64_t ret=from_be64(table->items[i].offset);
				//unused-mark is true
				if(get_H(ret)==1)
					ret = 0;	
			
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


void *btree_get(struct btree *btree, const uint8_t *sha1)
{
	uint64_t offset = lookup(btree, btree->top, sha1);
	if (offset == 0)
		return NULL;

	lseek64(btree->db_fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->db_fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;
	size_t len = from_be32(info.len);

	void *data = malloc(len);
	if (data == NULL)
		return NULL;
	if (read(btree->db_fd, data, len) != (ssize_t)len) {
		free(data);
		data = NULL;
	}
	return data;
}

void *btree_get_byoffset(struct btree *btree,uint64_t offset)
{
	if (offset == 0)
		return NULL;

	lseek64(btree->db_fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->db_fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;
	size_t len = from_be32(info.len);

	void *data = malloc(len);
	if (data == NULL)
		return NULL;
	if (read(btree->db_fd, data, len) != (ssize_t) len) {
		free(data);
		data = NULL;
	}
	return data;
}

int btree_delete(struct btree *btree, const uint8_t *c_sha1)
{
	uint8_t sha1[SHA1_LENGTH];
	memcpy(sha1, c_sha1, sizeof sha1);

	uint64_t offset = delete_table(btree, btree->top, sha1);
	if (offset == 0)
		return -1;
	return 0;
}

