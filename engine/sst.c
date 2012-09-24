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
#include "xmalloc.h"
#include "debug.h"

#define BLK_MAGIC (20111225)
#define F_CRC (2011)
#define GAP_SIZE (SST_MAX_COUNT * 2)

struct footer{
	char key[NESSDB_MAX_KEY_SIZE];
	uint32_t count;
	uint32_t crc;
};

static  void _make_sstname(int num, char *ret)
{
	snprintf(ret, FILE_NAME_SIZE, "%06d.sst", num);
}

void _add_bloom(struct sst *sst, char *sst_file, int fd, int count)
{
	int i;
	int blk_sizes;
	struct sst_block *blks;

	blk_sizes = count * sizeof(struct sst_block);
	blks = mmap(0, blk_sizes, PROT_READ, MAP_PRIVATE, fd, 0);
	if (blks == MAP_FAILED) {
		__PANIC("Error:Can't mmap file when add bloom");
		return;
	}

	for (i = 0; i < count; i++) 
		bloom_add(sst->bloom, blks[i].key);
	
	if (munmap(blks, blk_sizes) == -1)
		__ERROR("munmap file#%s error, %d, %s"
				, sst_file
				, errno
				, strerror(errno));
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
			if (result != sizeof(struct footer))
				__PANIC("read footer error");

			fcount = (footer.count);
			fcrc = (footer.crc);

			if (fcrc != F_CRC) {
				__PANIC("Crc wrong, sst file maybe broken, crc:<%d>,index<%s>", fcrc, sst_file);
				if (fd > 0)
					close(fd);
				continue;
			}

			if (fcount == 0) {
				if (fd > 0)
					close(fd);
				continue;
			}

			/* Add to bloom */
			_add_bloom(sst, sst_file, fd, fcount);

			all_count += fcount;
						
			/* Set meta */
			mn.count = fcount;
			memset(mn.end, 0, NESSDB_MAX_KEY_SIZE);
			memcpy(mn.end, footer.key, NESSDB_MAX_KEY_SIZE);

			memset(mn.name, 0, FILE_NAME_SIZE);
			memcpy(mn.name, de->d_name, FILE_NAME_SIZE);
			meta_set(sst->meta, &mn);
		
			if (fd > 0)
				close(fd);
		}
	}

	closedir(dd);
	__DEBUG("Load sst,all entries count:<%d>", all_count);
}

struct sst *sst_new(const char *basedir)
{
	struct sst *s;

	s = xcalloc(1, sizeof(struct sst));

	s->meta = meta_new();
	memcpy(s->basedir, basedir, FILE_PATH_SIZE);

	s->bloom = bloom_new();

	s->mutexer.lsn = -1;
	s->mutexer.mutex = xmalloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(s->mutexer.mutex, NULL);

	/* for remove hole */
	s->cpt = cpt_new();
	char db_name[FILE_PATH_SIZE];
	memset(db_name, 0, FILE_PATH_SIZE);
	snprintf(db_name, FILE_PATH_SIZE, "%s/%s", basedir, DB_NAME);
	s->db_fd = n_open(db_name, LSM_OPEN_FLAGS, 0644);
	if (s->db_fd == -1)
		__ERROR("fd is -1 when sst new");


	/* SST files load */
	_sst_load(s);
	
	return s;
}

void *_write_mmap(struct sst *sst, struct skipnode *x, size_t count, struct meta_node *mnode)
{
	int i, j, c_clone;
	int fd = -1;
	int sizes;
	int result;
	char file[FILE_PATH_SIZE];
	char sst_file[FILE_NAME_SIZE];
	struct skipnode *last;
	struct footer footer;
	struct sst_block *blks;


	if (mnode) {
#ifdef BGMERGE
		pthread_mutex_lock(sst->mutexer.mutex);
		sst->mutexer.lsn = mnode->lsn;
#endif
	}

	int fsize = sizeof(struct footer);
	memset(&footer, 0, fsize);
	sizes = count * sizeof(struct sst_block);


	memset(file, 0, FILE_PATH_SIZE);
	memset(sst_file, 0, FILE_NAME_SIZE);

	if (mnode) {
		memcpy(sst_file, mnode->name, strlen(mnode->name));
	} else{
		_make_sstname(sst->meta->size, sst_file);
	}
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, sst_file);

	fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd == -1) {
		__ERROR("creat sst file#%s", file);
		goto RET;
	}

	if (lseek(fd, sizes - 1, SEEK_SET) == -1) {
		__ERROR("lseek sst file#%s, sizes#%d", file, sizes);
		goto RET;
	}

	result = write(fd, "", 1);
	if (result == -1) {
		__ERROR("write empty to sst file#%s", file);
		goto RET;
	}

	blks = mmap(0, sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__ERROR("map sst file#%s", file);
		goto RET;
	}

	last = x;
	c_clone = count;
	for (i = 0, j = 0; i < c_clone; i++) {
		if (x->opt == ADD) {
			memset(blks[j].key, 0, NESSDB_MAX_KEY_SIZE);
			memcpy(blks[j].key, x->key, strlen(x->key));
			blks[j].offset = x->val;
			j++;
		} else {
#ifdef SHRINK
			/* add remove offsets to compact table */
			if (x->val > 0) {
				int val_len = 0;
				int result = lseek(sst->db_fd, x->val,  SEEK_SET);

				if (result > -1) {
					result = read(sst->db_fd, &val_len, sizeof(int));
					if (result > -1 && val_len > 0) {
						cpt_add(sst->cpt, val_len, x->val);
					}
				}
			}
#endif

			count--;
		}

		last = x;
		x = x->forward[0];
	}

#ifdef MSYNC
	if (msync(blks, sizes, MS_SYNC) == -1) {
		__ERROR("msync sst file#%s", file);
	}
#endif

	if (munmap(blks, sizes) == -1) {
		__ERROR("munmap sst file#%s", file);
	}
	
	footer.count = (count);
	footer.crc = (F_CRC);
	memcpy(footer.key, last->key, strlen(last->key));

	result = write(fd, &footer, fsize);
	if (result == -1) {
		__ERROR("write footer file#%s", file);
		goto RET;
	}

	/* Set meta */
	struct meta_node mn;

	mn.count = count;
	memset(mn.end, 0, NESSDB_MAX_KEY_SIZE);
	memcpy(mn.end, last->key, NESSDB_MAX_KEY_SIZE);

	memset(mn.name, 0, FILE_NAME_SIZE);
	memcpy(mn.name, sst_file, FILE_NAME_SIZE);
	
	if (!mnode)  {
		meta_set(sst->meta, &mn);
	} else {
		meta_set_byname(sst->meta, &mn);
	}

RET:
	if (fd > 0)
		close(fd);

	if (mnode) {
#ifdef BGMERGE
		pthread_mutex_unlock(sst->mutexer.mutex);
#endif
	}

	return x;
}

struct skiplist *_read_mmap(struct sst *sst, struct meta_node *mnode, size_t size)
{
	int i;
	int fd = -1;
	int result;
	int fcount;
	int blk_sizes;
	char file[FILE_PATH_SIZE];
	struct skiplist *merge = NULL;
	struct footer footer;
	struct sst_block *blks;
	int fsize = sizeof(struct footer);

	if (mnode) {
#ifdef BGMERGE
		pthread_mutex_lock(sst->mutexer.mutex);
		sst->mutexer.lsn = mnode->lsn;
#endif
	}

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, mnode->name);

	fd = open(file, O_RDWR, 0644);
	if (fd == -1) {
		__ERROR("open sst file#%s", file);
		return merge;
	}

	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		__ERROR("lseek footer sst file#%s", file);
		goto RET;
	}

	result = read(fd, &footer, fsize);
	if (result != fsize) {
		__ERROR("read footer sst file#%s", file);
		goto RET;
	}

	int fcrc = footer.crc;
	if (fcrc != F_CRC) {
		__ERROR("sst crc error when read mmap:fcrc#%d, sst#%s", fcrc, file);
		goto RET;
	}

	fcount = footer.count;
	blk_sizes = fcount * sizeof(struct sst_block);

	blks = mmap(0, blk_sizes, PROT_READ, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
		__ERROR("mmap sst file#%s", file);
		goto RET;
	}

	merge = skiplist_new(size);
	for (i = 0; i < fcount; i++) {
		skiplist_insert(merge, blks[i].key, blks[i].offset, ADD);
	}
	
	if (munmap(blks, blk_sizes) == -1) {
		__ERROR("munmap sst file#%s", file);
	}

RET:
	if (fd > 0)
		close(fd);

	if (mnode) {
#ifdef BGMERGE
		pthread_mutex_unlock(sst->mutexer.mutex);
#endif
	}

	return merge;
}

uint64_t _read_offset(struct sst *sst, struct slice *sk, struct meta_node *mnode)
{
	int fd = -1;
	int fcount;
	int result;
	uint64_t off = 0UL;
	char file[FILE_PATH_SIZE];
	struct footer footer;
	int fsize = sizeof(struct footer);
	struct sst_block blk;

	memset(file, 0, FILE_PATH_SIZE);
	snprintf(file, FILE_PATH_SIZE, "%s/%s", sst->basedir, mnode->name);

	fd = open(file, O_RDWR, 0644);
	if (fd == -1) {
		__ERROR("open sst file#%s", file);
		return 0UL;
	}
	
	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		__ERROR("lseek sst file#%s", file);
		goto RET;
	}

	result = read(fd, &footer, fsize);
	if (result == -1) {
		__ERROR("read footer sst file#%s", file);
		goto RET;
	}

	int fcrc = (footer.crc);
	if (fcrc != F_CRC) {
		__ERROR("sst crc error when read offset:fcrc#%d, sst#%s", fcrc, file);
		goto RET;
	}


	fcount = (footer.count);

	size_t left = 0, right = fcount, i = 0;
	while (left < right) {
		i = (right -left) / 2 +left;

		result = lseek(fd, i * sizeof(blk), SEEK_SET);
		result = read(fd, &blk, sizeof(blk));

		int cmp = strcmp(sk->data, blk.key);
		if (cmp == 0) {
			off = blk.offset;
			break ;
		}

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	
RET:
	if (fd > 0)
		close(fd);
	return off;
}

void _merge_list(struct skiplist *merge, struct skiplist *bemerge)
{
	struct skipnode *first = bemerge->hdr;
	struct skipnode *cur = bemerge->hdr->forward[0];

	while (cur != first) {
		skiplist_insert_node(merge, cur);
		cur = cur->forward[0];
	}
}

void _flush_merge_list(struct sst *sst, struct skiplist *block, struct meta_node *mnode)
{
	struct skipnode *x;

	/* NOTICE: merge_size must more than: GAP_SIZE + block_count */
	int merge_size = (block->count + GAP_SIZE + 1);
	struct skiplist * merge = _read_mmap(sst, mnode, merge_size);

	_merge_list(merge, block);

	x = merge->hdr->forward[0];
	if (merge->count <= GAP_SIZE) {
		x = _write_mmap(sst, x, merge->count, mnode);
	} else {
		int rem = merge->count % SST_MAX_COUNT;
		int mul = (merge->count - rem) / SST_MAX_COUNT;

		int i;
		for (i = 0; i < (mul - 1); i++) {
			x = _write_mmap(sst, x, SST_MAX_COUNT, mnode);
			mnode = NULL;
		}

		x = _write_mmap(sst, x, rem + SST_MAX_COUNT, NULL);
	}	
	if (merge)
		skiplist_free(merge);
}

void _flush_new_list(struct sst *sst, struct skipnode *x, size_t count)
{
	if (count <= GAP_SIZE) {
		x = _write_mmap(sst, x, count, NULL);
	} else {
		int rem = count % SST_MAX_COUNT;
		int mul = (count - rem) / SST_MAX_COUNT;

		int i;
		for (i = 0; i < (mul -1); i++) {
			x = _write_mmap(sst, x, SST_MAX_COUNT, NULL);
		}

		_write_mmap(sst, x, rem + SST_MAX_COUNT, NULL);
	}
}

#define _BLOCK_SIZE (3 * SST_MAX_COUNT)
void _flush_list(struct sst *sst, struct skipnode *x, struct skipnode *hdr, int flush_count)
{
	int pos = 0;
	int count = flush_count;
	struct skipnode *cur = x;
	struct meta_node *meta_cur = NULL;
	struct meta_node *meta_nxt = NULL;
	struct skiplist *block = skiplist_new(_BLOCK_SIZE);

	/*
	 * STEPS:
	 * 1) if meta_cur is NULL
	 *		a) flush the other nodes to disk as new sst
	 * 2) if meta_cur == meta_nxt, add current node to the blcok list(same sst)
	 *		a) if block size more than _BLOCK_SIZE, flush too
	 * 3) if meta_cur != meta_nxt, should flush the block to disk
	 * 4) flush the last block to disk if has node in it!
	 */
	while(cur != hdr) {
		meta_nxt = NULL;
		meta_cur = meta_get(sst->meta, cur->key);
		if (cur->forward[0] != hdr)
			meta_nxt = meta_get(sst->meta, cur->forward[0]->key);

		if(!meta_cur){
			if (block->count > 0) {
				_flush_merge_list(sst, block, meta_cur);
				skiplist_free(block);
				block = skiplist_new(_BLOCK_SIZE);
			}
			_flush_new_list(sst, cur, count - pos);
			goto ret;
		} else {
			skiplist_insert_node(block, cur);

			int cmp = -1;
			if (meta_nxt)
				cmp = strcmp(meta_nxt->name, meta_cur->name);

			if(cmp == 0) {
				if ( block->count == _BLOCK_SIZE) {
					_flush_merge_list(sst, block, meta_cur);
					skiplist_free(block);
					block = skiplist_new(_BLOCK_SIZE);
				}
			} else {
				if (block->count > 0) {
					_flush_merge_list(sst, block, meta_cur);
					skiplist_free(block);
					block = skiplist_new(_BLOCK_SIZE);
				}
			}
		}

		pos++;
		cur = cur->forward[0];
	}

	if (block->count > 0) {
		meta_cur = meta_get(sst->meta, block->hdr->forward[0]->key);
		_flush_merge_list(sst, block, meta_cur);
	}

ret:
	skiplist_free(block);
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
}

uint64_t sst_getoff(struct sst *sst, struct slice *sk)
{
	int lsn;
	uint64_t off = 0UL;
	struct meta_node *meta_cur;

	meta_cur = meta_get(sst->meta, sk->data);
	if(!meta_cur)
		return 0UL;

	/* If get one record from on-disk sst file, 
	 * this file must not be operated by bg-merge thread
	 */
	lsn = meta_cur->lsn;
	if (sst->mutexer.lsn == lsn) {
#ifdef BGMERGE
		pthread_mutex_lock(sst->mutexer.mutex);
#endif

		off = _read_offset(sst, sk, meta_cur);

#ifdef BGMERGE
		pthread_mutex_unlock(sst->mutexer.mutex);
#endif
	} else {
		off = _read_offset(sst, sk, meta_cur);
	}

	return off;
}

void sst_free(struct sst *sst)
{
	if (sst) {
		meta_free(sst->meta);
		bloom_free(sst->bloom);
		cpt_free(sst->cpt);
		free(sst->mutexer.mutex);
		free(sst);
	}
}
