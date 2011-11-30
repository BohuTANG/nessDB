 /*Based on Japeq's btree(https://github.com/japeq/btree_db)
 *BohuTANG did  a lot of changes, including:
 * 1) Separate the index and data files.
 * 2) Add tough Page-Cache,Random-Write and Random-Read very fast.
 * 3) Delete operation optimization,improve performance,very fast.
 * 4) Code abridged.
 * 5) And other dreams will go on if needed.
 * 6) Add Super Cache, and get  a "unmatched  speed"
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
 *   * Neither the name of nessDB nor the names of its contributors may be used
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

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS (64)

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include "storage.h"
#include "debug.h"

#define IDXEXT ".idx"
#define DBEXT ".db"
#define SUPEREXT ".super"


int _cmp_sha1(const char *a,const char *b)
{
	return strcmp(a,b);
}

struct btree_table *_alloc_table(struct btree *btree)
{
	struct btree_table *table = malloc(sizeof *table);
	memset(table, 0, sizeof *table);
	return table;
}

struct btree_table *_get_table(struct btree *btree, uint64_t offset)
{
	assert(offset != 0);

	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
	if (slot->offset == offset) {
		slot->offset = 0;
		return slot->table;
	}

	struct btree_table *table = malloc(sizeof *table);

	lseek(btree->fd, offset, SEEK_SET);
	if (read(btree->fd, table, sizeof *table) != (ssize_t) sizeof *table) {
		fprintf(stderr, "btree _get_table: I/O error\n");
		abort();
	}
	return table;
}

void _put_table(struct btree *btree, struct btree_table *table,
		      uint64_t offset)
{
	assert(offset != 0);

	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
		if (slot->offset != 0) {
		free(slot->table);
	}
	slot->offset = offset;
	slot->table = table;
}

void _flush_table(struct btree *btree, struct btree_table *table,
			uint64_t offset)
{
	assert(offset != 0);
	lseek(btree->fd, offset, SEEK_SET);
	if (write(btree->fd, table, sizeof *table) != (ssize_t) sizeof *table) {
		fprintf(stderr, "btree _flush_table: I/O error\n");
		abort();
	}
	_put_table(btree, table, offset);
}

void _flush_super(struct btree *btree)
{
	if (btree->top != btree->super_top) {
		struct btree_super super;
		memset(&super, 0, sizeof super);
		super.top = to_be64(btree->top);

		lseek(btree->super_fd, 0, SEEK_SET);
		if (write(btree->super_fd, &super, sizeof super) != sizeof super){
			fprintf(stderr, "btree flush super: I/O error\n");
			abort();
		}
		btree->super_top = btree->top;
	}
}

void _flush_magic(struct btree *btree)
{
	int magic=2011;
	if (write(btree->db_fd, &magic, sizeof magic) != sizeof magic){
		fprintf(stderr, "btree flush magic: I/O error\n");
		abort();
	}
}


int _btree_open(struct btree *btree, const char *idx, const char *db, const char *super_name)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open(idx, BTREE_OPEN_FLAGS);
	btree->db_fd = open(db, BTREE_OPEN_FLAGS);
	btree->super_fd = open(super_name, BTREE_OPEN_FLAGS);

	if (btree->fd < 0 || btree->db_fd<0)
		return -1;

	struct btree_super super;
	if (read(btree->super_fd, &super, sizeof super) != (ssize_t) sizeof super)
		return -1;
	btree->top = from_be64(super.top);

	__DEBUG("btree top is:%llu",btree->top);

	btree->super_top = btree->top;

	btree->alloc = lseek(btree->fd, 0, SEEK_END);
	btree->db_alloc = lseek(btree->db_fd, 0, SEEK_END);

	return 0;
}


int _btree_creat(struct btree *btree, const char *idx, const char *db, const char *super_name)
{
	memset(btree, 0, sizeof *btree);

	btree->fd = open(idx, BTREE_CREAT_FLAGS, 0644);
	btree->db_fd = open(db, BTREE_CREAT_FLAGS, 0644);
	btree->super_fd = open(super_name, BTREE_CREAT_FLAGS, 0644);

	if (btree->fd < 0 || btree->db_fd < 0 || btree->super_fd < 0)
		return -1;

	_flush_super(btree);

	btree->alloc = sizeof (struct btree_super);
	lseek(btree->fd, sizeof(struct btree_super), SEEK_SET);

	_flush_magic(btree);
	btree->db_alloc = sizeof(int);
	return 0;
}

int btree_init(struct btree *btree, const char *dbname)
{
	char idx[256];
	char db[256];
	char sup[256];
	int fd;

	snprintf(idx, sizeof idx, "%s%s", dbname, IDXEXT);
	snprintf(db, sizeof db, "%s%s", dbname, DBEXT);
	snprintf(sup, sizeof sup, "%s%s", dbname, SUPEREXT); 

	fd = open(idx, BTREE_OPEN_FLAGS, 0644);
	if ( fd > -1)
		_btree_open(btree, idx, db, sup);
	else {
		close(fd);
		_btree_creat(btree, idx, db, sup);
	}

	return (1);
}

void btree_close(struct btree *btree)
{
	size_t i;

	close(btree->fd);
	close(btree->db_fd);
	close(btree->super_fd);

	for (i = 0; i < CACHE_SLOTS; ++i) {
		if (btree->cache[i].offset)
			free(btree->cache[i].table);
	}
}

uint64_t _alloc_chunk(struct btree *btree, size_t len)
{
	uint64_t offset = btree->alloc;
	btree->alloc = offset + len;
	return offset;
}

uint64_t _alloc_db_chunk(struct btree *btree, size_t len)
{
	uint64_t offset  = btree->db_alloc;
	btree->db_alloc = offset + len;
	return offset;
}

uint64_t _insert_data(struct btree *btree, const void *data, size_t len)
{
	uint64_t offset;
	struct blob_info info;

	if (data == NULL)
		return len;

	memset(&info, 0, sizeof info);
	info.len = to_be32(len);

	offset = _alloc_db_chunk(btree, sizeof info + len);

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

uint64_t _split_table(struct btree *btree, struct btree_table *table,
			  char *sha1, uint64_t *offset)
{
	memcpy(sha1, table->items[TABLE_SIZE / 2].sha1, SHA1_LENGTH);
	*offset = from_be64(table->items[TABLE_SIZE / 2].offset);

	struct btree_table *new_table = _alloc_table(btree);
	new_table->size = table->size - TABLE_SIZE / 2 - 1;

	table->size = TABLE_SIZE / 2;

	memcpy(new_table->items, &table->items[TABLE_SIZE / 2 + 1],
		(new_table->size + 1) * sizeof(struct btree_item));

	uint64_t new_table_offset = _alloc_chunk(btree, sizeof *new_table);
	_flush_table(btree, new_table, new_table_offset);

	return new_table_offset;
}

uint64_t btree_insert_data(struct btree *btree, const void *data, size_t len)
{
	return _insert_data(btree, data, len);
}

uint64_t _insert_table(struct btree *btree, uint64_t table_offset,
			 char *sha1, uint64_t v_off)
{
	struct btree_table *table = _get_table(btree, table_offset);
	assert(table->size < TABLE_SIZE-1);

	size_t left = 0, right = table->size;
	while (left < right) {
		size_t i = (right - left) / 2 + left;
		int cmp = _cmp_sha1(sha1, table->items[i].sha1);
		if (cmp == 0) {
			/* already in the table update it*/
			uint64_t ret = v_off;
			table->items[i].offset= to_be64(ret);
			_flush_table(btree,table,table_offset);
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
		ret = _insert_table(btree, left_child, sha1, v_off);

		/* check if we need to split */
		struct btree_table *child = _get_table(btree, left_child);
		if (child->size < TABLE_SIZE-1) {
			/* nothing to do */
			_put_table(btree, table, table_offset);
			_put_table(btree, child, left_child);
			return ret;
		}
		/* overwrites SHA-1 */
		right_child = _split_table(btree, child, sha1, &offset);
		/* flush just in case changes happened */
		_flush_table(btree, child, left_child);
	} else
		ret=offset=v_off;

	table->size++;
	memmove(&table->items[i + 1], &table->items[i],
		(table->size - i) * sizeof(struct btree_item));
	memset(table->items[i].sha1,0,SHA1_LENGTH);
	memcpy(table->items[i].sha1, sha1, SHA1_LENGTH);
	table->items[i].offset = to_be64(offset);
	table->items[i].child = to_be64(left_child);
	table->items[i + 1].child = to_be64(right_child);

	_flush_table(btree, table, table_offset);
	return ret;
}


uint64_t _delete_table(struct btree *btree, uint64_t table_offset, char *sha1)
{
	while (table_offset) {
		struct btree_table *table = _get_table(btree, table_offset);
		size_t left = 0, right = table->size, i;
		while (left < right) {
			i = (right - left) / 2 + left;
			int cmp = _cmp_sha1(sha1, table->items[i].sha1);
			if (cmp == 0) {
				/* found */
				//mark unused
				uint64_t off = from_be64(table->items[i].offset);
				table->items[i].offset=to_be64(set64_H_1(off));
				_flush_table(btree, table, table_offset);
				return 1;
			}
			if (cmp < 0)
				right = i;
			else
				left = i + 1;
		}
		uint64_t  child = from_be64(table->items[left].child);
		_put_table(btree, table, table_offset);
		table_offset = child;
	}
	return 0;
}

uint64_t _insert_toplevel(struct btree *btree, uint64_t *table_offset,
			char *sha1, uint64_t v_off)
{
	uint64_t offset = 0;
	uint64_t ret = 0;
	uint64_t right_child = 0;
	if (*table_offset != 0) {
		ret = _insert_table(btree, *table_offset, sha1, v_off);

		/* check if we need to split */
		struct btree_table *table = _get_table(btree, *table_offset);
		if (table->size < TABLE_SIZE-1) {
			/* nothing to do */
			_put_table(btree, table, *table_offset);
			return ret;
		}
		right_child = _split_table(btree, table, sha1, &offset);
		_flush_table(btree, table, *table_offset);
	} else {
			ret = offset = v_off;
	}

	/* create new top level table */
	struct btree_table *new_table = _alloc_table(btree);
	new_table->size = 1;
	memcpy(new_table->items[0].sha1, sha1, SHA1_LENGTH);
	new_table->items[0].offset = to_be64(offset);
	new_table->items[0].child = to_be64(*table_offset);
	new_table->items[1].child = to_be64(right_child);

	uint64_t new_table_offset = _alloc_chunk(btree, sizeof *new_table);
	_flush_table(btree, new_table, new_table_offset);

	*table_offset = new_table_offset;
	return ret;
}


void  btree_insert_index(struct btree *btree,const char *c_sha1, uint64_t v_off)
{
	char sha1[SHA1_LENGTH];

	memset(sha1, 0, SHA1_LENGTH);
	memcpy(sha1,c_sha1,sizeof sha1);

	_insert_toplevel(btree, &btree->top, sha1, v_off);
	_flush_super(btree);
}


uint64_t _lookup(struct btree *btree, uint64_t table_offset, const char *sha1)
{
	while (table_offset) {
		struct btree_table *table = _get_table(btree, table_offset);
		size_t left = 0, right = table->size, i;
		while (left < right) {
			i = (right - left) / 2 +left;
			int cmp = _cmp_sha1((const char*)sha1, table->items[i].sha1);
			if (cmp == 0) {
				/* found */
				uint64_t ret=from_be64(table->items[i].offset);
				//unused-mark is true
				if(get64_H(ret)==1)
					ret = 0;	
			
				_put_table(btree, table, table_offset);
				return ret;
			}
			if (cmp < 0)
				right = i;
			else
				left = i + 1;
		}
		
		uint64_t child = from_be64(table->items[left].child);
		_put_table(btree, table, table_offset);
		table_offset = child;
	}
	return 0;
}


void *btree_get(struct btree *btree, const char *sha1)
{
	uint64_t offset = _lookup(btree, btree->top, sha1);
	
	if (offset == 0){
		return NULL;
	}

	lseek(btree->db_fd, offset, SEEK_SET);
	struct blob_info info;
	if (read(btree->db_fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;
	size_t len = from_be32(info.len);

	void *data = calloc(1,len);
	if (data == NULL)
		return NULL;
	if (read(btree->db_fd, data, len) != (ssize_t)len) {
		free(data);
		data = NULL;
	}
	return data;
}


int btree_get_index(struct btree *btree, const char *sha1)
{
	uint64_t offset = _lookup(btree, btree->top, sha1);
	if (offset == 0)
		return (0);
	return (1);
}

struct slice *btree_get_data(struct btree *btree,uint64_t offset)
{
	size_t len;
	char *data;
	struct blob_info info;
	struct slice *sv;

	if (offset == 0)
		return NULL;

	lseek(btree->db_fd, offset, SEEK_SET);
	if (read(btree->db_fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;

	len = from_be32(info.len);
	data = calloc(1,len);
	if (data == NULL)
		return NULL;

	if (read(btree->db_fd, data, len) != (ssize_t) len) {
		free(data);
		data = NULL;
	}
	sv = calloc(1, sizeof(struct slice));
	sv->len = len;
	sv->data = data;
	return sv;
}


int btree_delete(struct btree *btree, const char *c_sha1)
{
	char sha1[SHA1_LENGTH];

	memset(sha1, 0, SHA1_LENGTH);
	memcpy(sha1, c_sha1, sizeof sha1);

	uint64_t offset = _delete_table(btree, btree->top, sha1);
	if (offset == 0)
		return -1;
	return 0;
}

