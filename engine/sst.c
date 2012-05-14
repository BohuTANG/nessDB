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
#include "config.h"
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

#include "sst.h"
#include "debug.h"

#define BLK_MAGIC (20111225)
#define F_CRC (2011)

struct footer{
	char key[NESSDB_MAX_KEY_SIZE];
	__be32 count;
	__be32 crc;
	__be32 size;
	__be32 max_len;
	__be16 aligned;
	__be64 base_offset;
};

struct stats {
	int mmap_size;
	int max_len;
	uint64_t base;
	short aligned;
};

static inline int _aligned_bytes(uint64_t delta)
{
	int i;
	uint64_t _ua[] = {
		0xffffUL,
		0xffffffffUL,
		0xffffffffffffffff
	};

	for (i = 0; i < 3; i++) {
		if (delta <= _ua[i])
			break;
	}

	return 1<<i;
}

void _prepare_stats(struct skipnode *x, size_t count, struct stats *stats)
{
	size_t i;
	int real_count = 0;
	int max_len = 0;
	uint64_t min = 0UL;
	uint64_t max = 0UL;
	uint64_t delta = 0UL;

	struct skipnode *node = x;

	memset(stats, 0, sizeof(struct stats));
	for (i = 0; i < count; i++) {
		if (node->opt == ADD) {
			int klen = strlen(node->key);
			uint64_t off = from_be64(node->val);

			real_count++;
			max_len = klen > max_len ? klen : max_len;
			min = off < min ? off : min;
			max = off > max ? off : max;
		}

		node = node->forward[0];
	}

	stats->max_len = max_len;
	stats->mmap_size = (max_len + sizeof(uint64_t)) * real_count ;
	stats->base = min;
	delta = (max - min);
	stats->aligned = _aligned_bytes(delta);
}

void _add_bloom(struct sst *sst, int fd, int count, int max_len, short aligned)
{
	int i;
	int blk_sizes;
	struct inner_block{
		char key[max_len];
		char offset[aligned];
	};

	struct inner_block *blks;

	blk_sizes = count * sizeof(struct inner_block);

	blks= mmap(0, blk_sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__PANIC("Error:Can't mmap file when add bloom");
		return;
	}

	for (i = 0; i < count; i++) 
		bloom_add(sst->bloom, blks[i].key);
	
	if (munmap(blks, blk_sizes) == -1)
		__DEBUG(LEVEL_ERROR, "Error:un-mmapping the file");
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
			short aligned;

			memset(sst_file, 0, FILE_PATH_SIZE);
			snprintf(sst_file, FILE_PATH_SIZE, "%s/%s", sst->basedir, de->d_name);
			
			fd = open(sst_file, O_RDWR, 0644);
			lseek(fd, -fsize, SEEK_END);
			result = read(fd, &footer, sizeof(struct footer));
			if (result != sizeof(struct footer))
				__PANIC("read footer error");

			fcount = from_be32(footer.count);
			fcrc = from_be32(footer.crc);
			aligned = from_be16(footer.aligned);

			if (fcrc != F_CRC) {
				__PANIC("Crc wrong, sst file maybe broken, crc:<%d>,index<%s>", fcrc, sst_file);
				close(fd);
				continue;
			}

			if (fcount == 0) {
				close(fd);
				continue;
			}

			/* Add to bloom */
			_add_bloom(sst, fd, fcount, from_be32(footer.max_len), aligned);

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

	s = calloc(1, sizeof(struct sst));

	s->meta = meta_new();
	memcpy(s->basedir, basedir, FILE_PATH_SIZE);

	s->bloom = bloom_new();
	s->mutexer.lsn = -1;
	pthread_mutex_init(&s->mutexer.mutex, NULL);
	s->buf= buffer_new(1024*1024*4);

	/* SST files load */
	_sst_load(s);
	
	return s;
}

void *_write_mmap(struct sst *sst, struct skipnode *x, size_t count, int need_new)
{
	int i, j, c_clone;
	int fd;
	int sizes;
	int result;
	char file[FILE_PATH_SIZE];
	struct skipnode *last;
	struct footer footer;
	struct stats stats;

	int fsize = sizeof(struct footer);

	_prepare_stats(x, count, &stats);
	sizes = stats.mmap_size;

	struct inner_block {
		char key[stats.max_len];
		char offset[stats.aligned];
	};

	struct inner_block *blks;

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst->name);
	fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd == -1)
		__PANIC("create sst file error");

	if (lseek(fd, sizes - 1, SEEK_SET) == -1)
		__PANIC("lseek sst error");

	result = write(fd, "", 1);
	if (result == -1)
		__PANIC("write empty error");

	blks = mmap(0, sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__PANIC("map error when write");
	}

	last = x;
	c_clone = count;
	for (i = 0, j= 0; i < c_clone; i++) {
		if (x->opt == ADD) {
			buffer_putstr(sst->buf, x->key);
			if (stats.aligned == 8)
				buffer_putlong(sst->buf, x->val);
			else if (stats.aligned == 4)
				buffer_putint(sst->buf, x->val);
			else
				buffer_putshort(sst->buf, x->val);

			j++;
		} else
			count--;

		last = x;
		x = x->forward[0];
	}

	char *strings = buffer_detach(sst->buf);
	memcpy(blks, strings, sizes);

#ifdef MSYNC
	if (msync(blks, sizes, MS_SYNC) == -1) {
		__DEBUG(LEVEL_ERROR, "Msync error");
	}
#endif

	if (munmap(blks, sizes) == -1) {
		__DEBUG(LEVEL_ERROR, "Un-mmapping the file");
	}
	
	footer.count = to_be32(count);
	footer.crc = to_be32(F_CRC);
	footer.size = to_be32(sizes);
	footer.max_len = to_be32(stats.max_len);
	footer.aligned = to_be16(stats.aligned);
	memset(footer.key, 0, NESSDB_MAX_KEY_SIZE);
	memcpy(footer.key, last->key, strlen(last->key));

	result = write(fd, &footer, fsize);
	if (result == -1)
		__PANIC("write footer");

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
	struct skiplist *merge = NULL;
	struct footer footer;
	int fsize = sizeof(struct footer);

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst->name);

	fd = open(file, O_RDWR, 0644);
	if (fd == -1)
		__PANIC("open sst error when read map");

	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1)
		__PANIC("lseek footer  error");

	result = read(fd, &footer, fsize);
	if (result != fsize) {
		__PANIC("read error when read footer");
	}

	struct inner_block{
		char key[from_be32(footer.max_len)];
		char offset[from_be16(footer.aligned)];
	};

	struct inner_block *blks;

	fcount = from_be32(footer.count);
	blk_sizes = from_be32(footer.size);

	/* Blocks read */
	blks= mmap(0, blk_sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__PANIC("map error when read");
		goto out;
	}

	/* Merge */
	merge = skiplist_new(fcount + count + 1);
	for (i = 0; i < fcount; i++) {
		skiplist_insert(merge, blks[i].key, u64_from_big((unsigned char*)blks[i].offset), ADD);
	}
	
	if (munmap(blks, blk_sizes) == -1)
		__DEBUG(LEVEL_ERROR, "Un-mmapping the file");

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
	short aligned;
	uint64_t off = 0UL;
	char file[FILE_PATH_SIZE];
	struct footer footer;
	int fsize = sizeof(struct footer);

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst->name);

	fd = open(file, O_RDWR, 0644);
	if (fd == -1) {
		__DEBUG(LEVEL_ERROR, "open sst error when read offset");
		return 0UL;
	}
	
	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		__DEBUG(LEVEL_ERROR, "lseek error when read offset");
		close(fd);
		return off;
	}

	result = read(fd, &footer, fsize);
	if (result == -1) {
		__DEBUG(LEVEL_ERROR, "read footer error when read offset");
		close(fd);
		return off;
	}

	int max_len = from_be32(footer.max_len);
	aligned = from_be16(footer.aligned);

	struct inner_block {
		char key[max_len];
		char offset[aligned];
	};

	struct inner_block *blks;


	fcount = from_be32(footer.count);
	blk_sizes = from_be32(footer.size);

	/* Blocks read */
	blks= mmap(0, blk_sizes, PROT_READ, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__DEBUG(LEVEL_ERROR, "Map_failed when read");
		close(fd);
		return off;
	}

	size_t left = 0, right = fcount, i = 0;
	while (left < right) {
		i = (right -left) / 2 +left;
		int cmp = strcmp(sk->data, blks[i].key);
		if (cmp == 0) {
			if (aligned == 8)
				off = u64_from_big((unsigned char*)blks[i].offset);	
			else if (aligned == 4)
				off = u32_from_big((unsigned char*)blks[i].offset);	
			else
				off = u16_from_big((unsigned char*)blks[i].offset);	

			break ;
		}

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	
	if (munmap(blks, blk_sizes) == -1)
		__DEBUG(LEVEL_ERROR, "un-mmapping the file");

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

		__DEBUG(LEVEL_DEBUG, "adding log items to bloomfilter");
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
		buffer_free(sst->buf);
		free(sst);
	}
}
