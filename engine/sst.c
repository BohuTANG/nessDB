/* 
 * This is a simple 'sstable' implemention, merge all mtable into on-disk indices 
 * BLOCK's LAYOUT:
 * +--------+--------+--------+--------+
 * |             sst block 1           |
 * +--------+--------+--------+--------+
 * |             sst block 2           |
 * +--------+--------+--------+--------+
 * |      ... all the other blocks ..  |
 * +--------+--------+--------+--------+
 * |             sst block N           |
 * +--------+--------+--------+--------+
 * |             footer                |
 * +--------+--------+--------+--------+
 *
 * FOOTER's LAYOUT:
 * +--------+--------+--------+--------+
 * |               last key            |
 * +--------+--------+--------+--------+
 * |             block count           |
 * +--------+--------+--------+--------+
 * |                 crc               |
 * +--------+--------+--------+--------+

 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 * 
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>

#include "sst.h"
#include "debug.h"

#define BLK_MAGIC (20111225)
#define F_CRC (2011)

struct footer{
	char key[NESSDB_MAX_KEY_SIZE];
	__be32 count;
	__be32 crc;
};

void _add_bloom(struct sst *sst, int fd, int count)
{
	int i;
	int blk_sizes;
	struct sst_block *blks;

	blk_sizes = count * sizeof(struct sst_block);

	blks= mmap(0, blk_sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__DEBUG(LEVEL_ERROR, "%s", "add_bloom, mmapping the file");
		return;
	}

	for (i = 0; i < count; i++) 
		bloom_add(sst->bloom, blks[i].key);
	
	if (munmap(blks, blk_sizes) == -1)
		__DEBUG(LEVEL_ERROR, "%s", "Error:sst_bloom un-mmapping the file");
}

void _sst_load(struct sst *sst)
{
	int fd, result, all_count = 0;
	DIR *dd;
	struct dirent *de;

	dd = opendir(sst->basedir);
	while ((de = readdir(dd))) {
		if (strstr(de->d_name, ".sst")) {
			int fcount = 0, fcrc = 0;
			struct meta_node mn;
			struct footer footer;
			char sst_file[FILE_PATH_SIZE];
			int fsize = sizeof(struct footer);

			memset(sst_file, 0, FILE_PATH_SIZE);
			snprintf(sst_file, FILE_PATH_SIZE, "%s/%s", sst->basedir, de->d_name);
			
			fd = open(sst_file, O_RDWR, 0644);
			lseek(fd, -fsize, SEEK_END);
			result = read(fd, &footer, sizeof(struct footer));
			if (result == -1)
				abort();

			fcount = from_be32(footer.count);
			fcrc = from_be32(footer.crc);
			if (fcrc != F_CRC) {
				__DEBUG(LEVEL_ERROR, "Error:crc wrong,crc:<%d>,index<%s>", fcrc, sst_file);
				close(fd);
				continue;
			}

			if (fcount == 0) {
				close(fd);
				continue;
			}

			/* Add to bloom */
			_add_bloom(sst, fd, fcount);

			all_count += fcount;
						
			/* Set meta */
			mn.count = fcount;
			memset(mn.end, 0, NESSDB_MAX_KEY_SIZE);
			memcpy(mn.end, footer.key, NESSDB_MAX_KEY_SIZE);

			memset(mn.index_name, 0, FILE_NAME_SIZE);
			memcpy(mn.index_name, de->d_name, FILE_NAME_SIZE);
			meta_set(sst->meta, &mn);
		
			close(fd);
		}
	}
	closedir(dd);
	__DEBUG(LEVEL_DEBUG, "Load sst,all entries count:<%d>", all_count);
}

struct sst *sst_new(const char *basedir)
{
	struct sst *s;

	s = malloc(sizeof(struct sst));
	s->lsn = 0;

	s->meta = meta_new();
	memcpy(s->basedir, basedir, FILE_PATH_SIZE);

	s->bloom = bloom_new();
	s->mutexer.lsn = -1;
	pthread_mutex_init(&s->mutexer.mutex, NULL);

	/* SST files load */
	_sst_load(s);
	
	return s;
}

/*
 * Write node to index file
 */
void *_write_mmap(struct sst *sst, struct skipnode *x, size_t count, int need_new)
{
	int i, j, c_clone;
	int fd;
	int sizes;
	int result;
	char file[FILE_PATH_SIZE];
	struct skipnode *last;
	struct sst_block *blks;
	struct footer footer;

	int fsize = sizeof(struct footer);

	sizes = count * sizeof(struct sst_block);

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst->name);
	fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);

	lseek(fd, sizes - 1, SEEK_SET);
	result = write(fd, "", 1);
	if (result == -1)
		abort();

	blks = mmap(0, sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

	last = x;
	c_clone = count;
	for (i = 0, j= 0; i < c_clone; i++) {
		if (x->opt == ADD) {
			memset(blks[j].key, 0, NESSDB_MAX_KEY_SIZE);
			memcpy(blks[j].key, x->key,NESSDB_MAX_KEY_SIZE);
			blks[j].offset=to_be64(x->val);
			j++;
		} else
			count--;

		last = x;
		x = x->forward[0];
	}

#ifdef MSYNC
	if (msync(blks, sizes, MS_SYNC) == -1) {
		__DEBUG(LEVEL_ERROR, "%s", "write_map: msync error");
	}
#endif

	if (munmap(blks, sizes) == -1) {
		__DEBUG(LEVEL_ERROR, "%s", "write_map:un-mmapping the file");
	}
	
	footer.count = to_be32(count);
	footer.crc = to_be32(F_CRC);
	memset(footer.key, 0, NESSDB_MAX_KEY_SIZE);
	memcpy(footer.key, last->key, NESSDB_MAX_KEY_SIZE);

	result = write(fd, &footer, fsize);
	if (result == -1)
		abort();

	/* Set meta */
	struct meta_node mn;

	mn.count = count;
	memset(mn.end, 0, NESSDB_MAX_KEY_SIZE);
	memcpy(mn.end, last->key, NESSDB_MAX_KEY_SIZE);

	memset(mn.index_name, 0, FILE_NAME_SIZE);
	memcpy(mn.index_name, sst->name, FILE_NAME_SIZE);
	
	if (need_new) 
		meta_set(sst->meta, &mn);
	else 
		meta_set_byname(sst->meta, &mn);

	close(fd);

	return x;
}

struct skiplist *_read_mmap(struct sst *sst, size_t count)
{
	int i;
	int fd;
	int result;
	int fcount;
	int blk_sizes;
	char file[FILE_PATH_SIZE];
	struct sst_block *blks;
	struct skiplist *merge = NULL;
	struct footer footer;
	int fsize = sizeof(struct footer);

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst->name);

	fd = open(file, O_RDWR, 0644);
	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		abort();
	}
	result = read(fd, &footer, fsize);
	if (result == -1)
		abort();

	fcount = from_be32(footer.count);

	blk_sizes = fcount * sizeof(struct sst_block);

	/* Blocks read */
	blks= mmap(0, blk_sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__DEBUG(LEVEL_ERROR, "%s", "Error:sst_bloom un-mmapping the file");
		goto out;
	}

	/* Merge */
	merge = skiplist_new(fcount + count + 1);
	for (i = 0; i < fcount; i++) {
			skiplist_insert(merge, blks[i].key, from_be64(blks[i].offset), ADD);
	}
	
	if (munmap(blks, blk_sizes) == -1)
		__DEBUG(LEVEL_ERROR, "%s", "read_map:un-mmapping the file");

out:
	close(fd);

	return merge;
}

uint64_t _read_offset(struct sst *sst, struct slice *sk)
{
	int fd;
	int fcount;
	int blk_sizes;
	int result;
	uint64_t off = 0UL;
	char file[FILE_PATH_SIZE];
	struct sst_block *blks;
	struct footer footer;
	int fsize = sizeof(struct footer);

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst->name);

	fd = open(file, O_RDWR, 0644);
	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		abort();
	}
	result = read(fd, &footer, fsize);
	if (result == -1)
		abort();

	fcount = from_be32(footer.count);
	blk_sizes = fcount * sizeof(struct sst_block);

	/* Blocks read */
	blks= mmap(0, blk_sizes, PROT_READ, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__DEBUG(LEVEL_ERROR, "%s", "Error:read_offset, mmapping the file");
		goto out;
	}

	size_t left = 0, right = fcount, i = 0;
	while (left < right) {
		i = (right -left) / 2 +left;
		int cmp = strcmp(sk->data, blks[i].key);
		if (cmp == 0) {
			off = from_be64(blks[i].offset);	
			break ;
		}

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	
	if (munmap(blks, blk_sizes) == -1)
		__DEBUG(LEVEL_ERROR, "%s", "read_offset:un-mmapping the file");

out:
	close(fd);

	return off;
}

void _flush_merge_list(struct sst *sst, struct skipnode *x, size_t count, struct meta_node *meta)
{
	int mul;
	int rem;
	int lsn;
	int i;

	/* Less than 2x SST_MAX_COUNT,compact one index file */
	if (count <= SST_MAX_COUNT * 2) {
		if (meta) {
			lsn = meta->lsn;
			sst->mutexer.lsn = lsn;
			pthread_mutex_lock(&sst->mutexer.mutex);
			x = _write_mmap(sst, x, count, 0);
			pthread_mutex_unlock(&sst->mutexer.mutex);
			sst->mutexer.lsn = -1;
		} else 
			x = _write_mmap(sst, x, count, 0);
	} else {
		if (meta) {
			lsn = meta->lsn;
			sst->mutexer.lsn = lsn;
			pthread_mutex_lock(&sst->mutexer.mutex);
			x = _write_mmap(sst, x, SST_MAX_COUNT, 0);
			pthread_mutex_unlock(&sst->mutexer.mutex);
			sst->mutexer.lsn = -1;
		} else
			x = _write_mmap(sst, x, SST_MAX_COUNT, 0);

		/* first+last */
		mul = (count - SST_MAX_COUNT * 2) / SST_MAX_COUNT;
		rem = count % SST_MAX_COUNT;

		for (i = 0; i < mul; i++) {
			memset(sst->name, 0, FILE_NAME_SIZE);
			snprintf(sst->name, FILE_NAME_SIZE, "%d.sst", sst->meta->size); 
			x = _write_mmap(sst, x, SST_MAX_COUNT, 1);
		}

		/* The remain part,will be larger than SST_MAX_COUNT */
		memset(sst->name, 0, FILE_NAME_SIZE);
		snprintf(sst->name, FILE_NAME_SIZE, "%d.sst", sst->meta->size); 

		x = _write_mmap(sst, x, rem + SST_MAX_COUNT, 1);
	}	
}

void _flush_new_list(struct sst *sst, struct skipnode *x, size_t count)
{
	int mul ;
	int rem;
	int i;

	if (count <= SST_MAX_COUNT * 2) {
		memset(sst->name, 0, FILE_NAME_SIZE);
		snprintf(sst->name, FILE_NAME_SIZE, "%d.sst", sst->meta->size); 
		x = _write_mmap(sst, x, count, 1);
	} else {
		mul = count / SST_MAX_COUNT;
		rem = count % SST_MAX_COUNT;

		for (i = 0; i < (mul - 1); i++) {
			memset(sst->name, 0, FILE_NAME_SIZE);
			snprintf(sst->name, FILE_NAME_SIZE, "%d.sst", sst->meta->size); 
			x = _write_mmap(sst, x, SST_MAX_COUNT, 1);
		}

		memset(sst->name, 0, FILE_NAME_SIZE);
		snprintf(sst->name, FILE_NAME_SIZE, "%d.sst", sst->meta->size); 
		x = _write_mmap(sst, x, SST_MAX_COUNT + rem, 1);
	}
}

void _flush_list(struct sst *sst, struct skipnode *x,struct skipnode *hdr,int flush_count)
{
	int pos = 0;
	int count = flush_count;
	struct skipnode *cur = x;
	struct skipnode *first = hdr;
	struct skiplist *merge = NULL;
	struct meta_node *meta_info = NULL;

	while(cur != first) {
		meta_info = meta_get(sst->meta, cur->key);

		/* If m is NULL, cur->key more larger than meta's largest area
		 * need to create new index-file
		 */
		if(!meta_info){

			/* If merge is NULL,it has no merge*/
			if(merge) {
				struct skipnode *h = merge->hdr->forward[0];
				_flush_merge_list(sst, h, merge->count, NULL);
				skiplist_free(merge);
				merge = NULL;
			}

			/* Flush the last nodes to disk */
			_flush_new_list(sst, x, count - pos);

			return;
		} else {

			/* If m is not NULL,means found the index of the cur
			 * We need:
			 * 1) compare the sst->name with meta index name
			 *		a)If 0: add the cur to merge,and continue
			 *		b)others:
			 *			b1)Flush the merge list to disk
			 *			b2)Open the meta's mmap,and load all blocks to new merge,add cur to merge
			 */
			int cmp = strcmp(sst->name, meta_info->index_name);
			if(cmp == 0) {
				if (!merge)
					merge = _read_mmap(sst,count);	

				skiplist_insert_node(merge, cur);
			} else {
				if (merge) {
					struct skipnode *h = merge->hdr->forward[0];

					_flush_merge_list(sst, h, merge->count, meta_info);
					skiplist_free(merge);
					merge = NULL;
				}

				memset(sst->name, 0, FILE_NAME_SIZE);
				memcpy(sst->name, meta_info->index_name, FILE_NAME_SIZE);
				merge = _read_mmap(sst, count);

				/* Add to merge list */
				skiplist_insert_node(merge, cur);
			}

		}

		pos++;
		cur = cur->forward[0];
	}

	if (merge) {
		struct skipnode *h = merge->hdr->forward[0];
		_flush_merge_list(sst, h, merge->count, meta_info);
		skiplist_free(merge);
	}
}

void sst_merge(struct sst *sst, struct skiplist *list, int fromlog)
{
	struct skipnode *x= list->hdr->forward[0];

	if (fromlog == 1) {
		struct skipnode *cur = x;
		struct skipnode *first = list->hdr;

		__DEBUG(LEVEL_DEBUG, "%s", "adding log items to bloomfilter");
		while (cur != first) {
			if (cur->opt == ADD)
				bloom_add(sst->bloom, cur->key);
			cur = cur->forward[0];
		}
	}

	/* First time,index is NULL,need to be created */
	if (sst->meta->size == 0)
		 _flush_new_list(sst, x, list->count);
	else
		_flush_list(sst, x, list->hdr, list->count);
	skiplist_free(list);
}

uint64_t sst_getoff(struct sst *sst, struct slice *sk)
{
	int lsn;
	uint64_t off = 0UL;
	struct meta_node *meta_info;

	meta_info = meta_get(sst->meta, sk->data);
	if(!meta_info)
		return 0UL;

	memcpy(sst->name, meta_info->index_name, FILE_NAME_SIZE);

	/* If get one record from on-disk sst file, 
	 * this file must not be operated by bg-merge thread
	 */
	lsn = meta_info->lsn;
	if (sst->mutexer.lsn == lsn) {
		pthread_mutex_lock(&sst->mutexer.mutex);
		off = _read_offset(sst, sk);
		pthread_mutex_unlock(&sst->mutexer.mutex);
	} else {
		off = _read_offset(sst, sk);
	}

	return off;
}

void sst_free(struct sst *sst)
{
	if (sst) {
		meta_free(sst->meta);
		bloom_free(sst->bloom);
		free(sst);
	}
}
