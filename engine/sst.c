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

 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
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

#define SST_MAX (25000)
#define BLK_MAGIC (20111225)
#define F_CRC (2011)

struct footer{
	char key[SKIP_KSIZE];
	int count;
	int crc;
};

void _sst_load(struct sst *sst)
{
	int fd;
	int all_count = 0;
	int result;
	DIR *dd;
	struct dirent *de;

	dd = opendir(sst->basedir);
	while ((de = readdir(dd)) != NULL) {
		if (strstr(de->d_name, ".sst")) {
			struct meta_node mn;
			struct footer footer;
			char sst_file[SST_FLEN];
			int fsize = sizeof(struct footer);


			memset(sst_file, 0, SST_FLEN);
			snprintf(sst_file, SST_FLEN, "%s/%s", sst->basedir, de->d_name);
			
			fd = open(sst_file, O_RDWR, 0644);
			lseek(fd, -fsize, SEEK_END);
			result = read(fd, &footer, sizeof(struct footer));
			if (result == -1)
				abort();

			if (footer.crc != F_CRC) {
				__DEBUG("Error:crc wrong,crc:<%d>,index<%s>", footer.crc, sst_file);
				close(fd);
				continue;
			}

			all_count += footer.count;
						
			/* Set meta */
			mn.count = footer.count;
			memset(mn.end, 0, SKIP_KSIZE);
			memcpy(mn.end, footer.key, SKIP_KSIZE);

			memset(mn.index_name, 0, SST_NSIZE);
			memcpy(mn.index_name, de->d_name, SST_NSIZE);
			meta_set(sst->meta, &mn);
		
			close(fd);
		}
	}
	closedir(dd);
	__DEBUG("Load sst,all entries count:<%d>", all_count);
}

struct sst *sst_new(const char *basedir)
{
	struct sst *s;

	s = malloc(sizeof(struct sst));
	s->lsn = 0;

	s->meta = meta_new();
	memcpy(s->basedir, basedir, SST_FLEN);

	/* SST files load */
	_sst_load(s);
	
	return s;
}
/*
 * Write node to index file
 */
void *_write_mmap(struct sst *sst, struct skipnode *x, size_t count, int need_new)
{
	int i;
	int fd;
	int sizes;
	int result;
	char file[SST_FLEN];
	struct skipnode *last;
	struct sst_block *blks;
	struct footer footer;

	int fsize = sizeof(struct footer);

	sizes = count * sizeof(struct sst_block);

	memset(file, 0, SST_FLEN);
	snprintf(file, SST_FLEN, "%s/%s", sst->basedir, sst->name);
	fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);

	lseek(fd, sizes - 1, SEEK_SET);
	result = write(fd, "", 1);
	if (result == -1)
		abort();

	blks = mmap(0, sizes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

	last = x;
	for (i = 0 ; i < count; i++) {
		if (blks[i].opt == ADD) {
			memcpy(blks[i].key, x->key,SKIP_KSIZE);
			blks[i].offset=x->val;
			blks[i].opt=x->opt;
		} else
			count--;

		last = x;
		x = x->forward[0];
	}

	if (munmap(blks, sizes) == -1) {
		perror("Error un-mmapping the file");
	}
	
	footer.count = count;
	footer.crc = F_CRC;
	memset(footer.key, 0, SKIP_KSIZE);
	memcpy(footer.key, last->key, SKIP_KSIZE);

	result = write(fd,&footer, fsize);
	if (result == -1)
		abort();

	/* Set meta */
	struct meta_node mn;

	mn.count = count;
	memset(mn.end, 0, SKIP_KSIZE);
	memcpy(mn.end, last->key, SKIP_KSIZE);

	memset(mn.index_name, 0, SST_NSIZE);
	memcpy(mn.index_name, sst->name, SST_NSIZE);
	
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
	int blk_sizes;
	char file[SST_FLEN];
	struct sst_block *blks;
	struct skiplist *merge = NULL;
	struct footer footer;
	int fsize = sizeof(struct footer);

	memset(file, 0, SST_FLEN);
	snprintf(file, SST_FLEN, "%s/%s", sst->basedir, sst->name);

	fd = open(file, O_RDWR, 0644);
	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		abort();
	}
	result = read(fd, &footer, fsize);
	if (result == -1)
		abort();

	blk_sizes = footer.count * sizeof(struct sst_block);

	/* Blocks read */
	blks= mmap(0, blk_sizes, PROT_READ, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
	    perror("Error:read_mmap, mmapping the file");
		goto out;
	}

	/* Merge */
	merge = skiplist_new(footer.count + count + 1);
	for (i = 0; i < footer.count; i++) {
		if (blks[i].opt == ADD)
			skiplist_insert(merge, blks[i].key, blks[i].offset, ADD);
	}
	
	if (munmap(blks, blk_sizes) == -1)
		perror("Error:read_mmap un-mmapping the file");

out:
	close(fd);

	return merge;
}

uint64_t _read_offset(struct sst *sst, const char *key)
{
	int fd;
	int blk_sizes;
	int result;
	uint64_t off = 0UL;
	char file[SST_FLEN];
	struct sst_block *blks;
	struct footer footer;
	int fsize = sizeof(struct footer);

	memset(file, 0, SST_FLEN);
	snprintf(file, SST_FLEN, "%s/%s", sst->basedir, sst->name);

	fd = open(file, O_RDWR, 0644);
	result = lseek(fd, -fsize, SEEK_END);
	if (result == -1) {
		abort();
	}
	result = read(fd, &footer, fsize);
	if (result == -1)
		abort();

	blk_sizes = footer.count * sizeof(struct sst_block);

	/* Blocks read */
	blks= mmap(0, blk_sizes, PROT_READ, MAP_SHARED, fd, 0);
	if (blks == MAP_FAILED) {
	    perror("Error:read_offset, mmapping the file");
		goto out;
	}

	size_t left = 0, right = footer.count, i = 0;
	while (left < right) {
		i = (right -left) / 2 +left;
		int cmp = strcmp(key, blks[i].key);
		if (cmp == 0) {
			if (blks[i].opt == ADD)
				off = blks[i].offset;	
			break ;
		}

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	
	if (munmap(blks, blk_sizes) == -1)
		perror("Error:read_offset, un-mmapping the file");

out:
	close(fd);

	return off;
}

void _flush_merge_list(struct sst *sst, struct skipnode *x, size_t count)
{
	int mul;
	int rem;

	/* Less than 2x SST_MAX,compact one index file */
	if (count <= SST_MAX * 2) {
		x = _write_mmap(sst, x, count, 0);
	} else {
		x = _write_mmap(sst, x, SST_MAX, 0);

		/* first+last */
		mul = (count - SST_MAX * 2) / SST_MAX;
		rem = count % SST_MAX;

		for (int i = 0; i < mul; i++) {
			memset(sst->name, 0, SST_NSIZE);
			snprintf(sst->name, SST_NSIZE, "%d.sst", sst->meta->size); 
			x = _write_mmap(sst, x, SST_MAX, 1);
		}

		/* The remain part,will be larger than SST_MAX */
		memset(sst->name, 0, SST_NSIZE);
		snprintf(sst->name, SST_NSIZE, "%d.sst", sst->meta->size); 

		x = _write_mmap(sst, x, rem + SST_MAX, 1);
	}	
}

void _flush_new_list(struct sst *sst, struct skipnode *x, size_t count)
{
	int mul ;
	int rem;

	if (count <= SST_MAX * 2) {
		memset(sst->name, 0, SST_NSIZE);
		snprintf(sst->name, SST_NSIZE, "%d.sst", sst->meta->size); 
		x = _write_mmap(sst, x, count, 1);
	} else {
		mul = count / SST_MAX;
		rem = count % SST_MAX;

		for (int i = 0; i < (mul - 1); i++) {
			memset(sst->name, 0, SST_NSIZE);
			snprintf(sst->name, SST_NSIZE, "%d.sst", sst->meta->size); 
			x = _write_mmap(sst, x, SST_MAX, 1);
		}

		memset(sst->name, 0, SST_NSIZE);
		snprintf(sst->name, SST_NSIZE, "%d.sst", sst->meta->size); 
		x = _write_mmap(sst, x, SST_MAX + rem, 1);
	}
}

void _flush_list(struct sst *sst, struct skipnode *x,struct skipnode *hdr,int flush_count)
{
	int pos = 0;
	int count = flush_count;
	struct skipnode *cur = x;
	struct skipnode *first = hdr;
	struct skiplist *merge = NULL;

	while(cur != first) {

		struct meta_node *meta_info = meta_get(sst->meta, cur->key);

		/* If m is NULL, cur->key more larger than meta's largest area
		 * need to create new index-file
		 */
		if(meta_info == NULL){

			/* If merge is NULL,it has no merge*/
			if(merge != NULL) {
				struct skipnode *h = merge->hdr->forward[0];
				_flush_merge_list(sst, h, merge->count);
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
				if (merge == NULL)
					merge = _read_mmap(sst,count);	

				skiplist_insert_node(merge, cur);
			} else {
				if (merge) {
					struct skipnode *h = merge->hdr->forward[0];

					_flush_merge_list(sst, h, merge->count);
					skiplist_free(merge);
					merge = NULL;
				}

				memset(sst->name, 0, SST_NSIZE);
				memcpy(sst->name, meta_info->index_name, SST_NSIZE);
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
		_flush_merge_list(sst, h, merge->count);
		skiplist_free(merge);
	}
}

void sst_merge(struct sst *sst, struct skiplist *list)
{
	struct skipnode *x= list->hdr->forward[0];

	/* First time,index is NULL,need to be created */
	if (sst->meta->size == 0)
		 _flush_new_list(sst, x, list->count);
	else
		_flush_list(sst, x, list->hdr, list->count);

}

uint64_t sst_getoff(struct sst *sst, struct slice *sk)
{
	/* Have some bugs to fixed */
	uint64_t off = 0UL;
	struct meta_node *meta_info;

	meta_info = meta_get(sst->meta, sk->data);
	if(meta_info == NULL)
		return 0UL;

	memcpy(sst->name, meta_info->index_name, SST_NSIZE);
	off = _read_offset(sst, sk->data);

	return off;
}

void sst_free(struct sst *sst)
{
	if (sst)
		free(sst);
}
