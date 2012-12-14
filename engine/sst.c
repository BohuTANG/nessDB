/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * This is Bε-tree with ε=1/2, using array as LEVELs, seems like COLAS(Cache Oblivious Lookahead Arrays).
 * The main algorithm described is here: spec/small-splittable-tree.txt
 */

#include "sst.h"
#include "debug.h"
#include "xmalloc.h"

void _insertion_sort(struct sst_item *item, int len)
{
	int i, j;

	for (i = 0; i < len; i++) {
		int cmp;
		struct sst_item v = item[i];

		for (j = i - 1; j >= 0; j--) {
			cmp = strcmp(item[j].data, v.data);
			if (cmp <= 0) {

				/* covert the old version */
				if (cmp == 0)
					memcpy(&item[j], &v, ITEM_SIZE); 

				break;
			}

			memcpy(&item[j + 1], &item[j], ITEM_SIZE);
		}
		memcpy(&item[j + 1], &v, ITEM_SIZE);
	}
}

int _merge_sort(struct compact *cpt, struct sst_item *c,
				   struct sst_item *a_new, int alen,
				   struct sst_item *b_old, int blen)
{
	int i, m = 0, n = 0, k;

	for (i = 0; (m < alen) && (n < blen);) {
		int cmp;

		cmp = strcmp(a_new[m].data, b_old[n].data);
		if (cmp == 0) {
			memcpy(&c[i++], &a_new[m], ITEM_SIZE);

			/* add delete slot to cpt */
			if (a_new[m].opt == 0 && a_new[m].vlen > 0) 
				if (cpt)
					cpt_add(cpt, a_new[m].vlen, a_new[m].offset); 
			n++;
			m++;
		} else if (cmp < 0) 
			memcpy(&c[i++], &a_new[m++], ITEM_SIZE);
		else 
			memcpy(&c[i++], &b_old[n++], ITEM_SIZE);
	}

	if (m == alen) {
		for (k = n; k < blen; k++)
			memcpy(&c[i++], &b_old[k], ITEM_SIZE);
	} else if (n == blen) { 
		for (k = m; k < alen; k++)
			memcpy(&c[i++], &a_new[k], ITEM_SIZE);
	}

	return i;
}

/* 
 * calc level's offset of file 
 */
int _pos_calc(int level)
{
	int i = 0;
	int off = HEADER_SIZE;
	
	while(i < level) {
		off += (pow(LEVEL_BASE, i) * L0_SIZE);
		i++;
	}

	return off;
}

void _update_header(struct sst *sst)
{
	int res;

	res = pwrite(sst->fd, &sst->header, HEADER_SIZE, 0);
	if (res == -1)
		return;
}

int _level_max(int level, int gap)
{
	return (int)(pow(LEVEL_BASE, level) * L0_SIZE / ITEM_SIZE - gap);
}

void sst_dump(struct sst *sst) {
	int i;

	__DEBUG("**%06d.SST dump:", 
			sst->fd);

	for(i = 0; i< (int)MAX_LEVEL; i++) {
		printf("\t\t-L#%d---count#%d, max-count:%d\n",
				i,
				sst->header.count[i],
				(int)_level_max(i, 0));
	}
	printf("\n");
}

struct sst_item * read_one_level(struct sst *sst, int level, int readc)
{
	int res;
	int c = sst->header.count[level];
	struct sst_item *L = xcalloc(readc + 1, ITEM_SIZE);

	if (c > 0) {
		res = pread(sst->fd, L, readc * ITEM_SIZE, _pos_calc(level) + (c - readc) * ITEM_SIZE);
		if (res == -1)
			__PANIC("read klen error");

		if (level == 0)
			_insertion_sort(L, readc);
	}

	return L;
}

void write_one_level(struct sst *sst, struct sst_item *L, int count, int level)
{
	int res;

	res = pwrite(sst->fd, L, count * ITEM_SIZE, _pos_calc(level));
	if (res == -1)
		__PANIC("write to one level....");
}

void  _merge_to_next(struct sst *sst, int level, int mergec) 
{
	int c2 = sst->header.count[level + 1];
	int lmerge_c = mergec + c2;
	struct sst_item *L = read_one_level(sst, level, mergec);
	struct sst_item *L_nxt = read_one_level(sst, level + 1, c2);
	struct sst_item *L_merge = xcalloc(lmerge_c + 1, ITEM_SIZE);

	lmerge_c = _merge_sort(sst->cpt, L_merge, L, mergec, L_nxt, c2);
	write_one_level(sst, L_merge, lmerge_c, level + 1);

	/* update count */
	sst->header.count[level] -= mergec;
	sst->header.count[level + 1] = lmerge_c;

	_update_header(sst);

	xfree(L);
	xfree(L_nxt);
	xfree(L_merge);

	sst->stats->STATS_LEVEL_MERGES++;
} 

void _check_merge(struct sst *sst)
{
	int i;
	int c;
	int max;
	int nxt_c = 0;
	int nxt_max = 0;
	int full = 0;

	for (i = MAX_LEVEL - 1; i >= 0; i--) {
		c = sst->header.count[i];
		max = _level_max(i, 3);

		/* bottom level */
		if (i == (MAX_LEVEL - 1)) {
			if (c >= max)
				full++;
			continue;
		} else {
			nxt_c = sst->header.count[i + 1];
			nxt_max = _level_max(i + 1, 3);
			if (nxt_c >= nxt_max) {
				full++;
				continue;
			}
		}

		if (c >= max) {
			int diff = nxt_max - (c + nxt_c);

			/* merge full level to next level */
			if (diff >= 0) {
				_merge_to_next(sst, i, c);
			} else {
				diff = nxt_max - nxt_c;
				if (diff > 0)
					_merge_to_next(sst, i, diff);
			}
		}
	} 

	if (full >= (MAX_LEVEL - 1)) {
		__DEBUG("--all levels[%d] is full#%d, need to be scryed...", 
				MAX_LEVEL - 1,  
				full);

		sst->willfull = 1;
		sst_dump(sst);
	}
}

void _build_block(struct sst *sst)
{
	int i;

	for (i = 0; i < MAX_LEVEL; i++) {
		int c;
		struct sst_item *L;

		c = sst->header.count[i];
		L = read_one_level(sst, i, c);
		if (c > 0)
			block_build(sst->blk, L, c, i); 
		xfree(L);
	}
}

#define BLOCK0_COUNT ((L0_SIZE/ITEM_SIZE)/BLOCK_GAP + 1)
struct sst *sst_new(const char *file, struct compact *cpt, struct stats *stats)
{
	int res;
	struct sst *sst = xcalloc(1, sizeof(struct sst));

	sst->oneblk = xcalloc(BLOCK_GAP, ITEM_SIZE);
	sst->blk= block_new(BLOCK0_COUNT);

	sst->fd = n_open(file, N_OPEN_FLAGS, 0644);
	if (sst->fd > 0) {
		res = pread(sst->fd, &sst->header, HEADER_SIZE, 0);
		if (res == -1)
			goto ERR;

		_build_block(sst);
	} else
		sst->fd = n_open(file, N_CREAT_FLAGS, 0644);

	sst->stats = stats;
	sst->bf = bloom_new(sst->header.bitset);
	if (cpt != NULL)
		sst->cpt = cpt;

	return sst;

ERR:
	if (sst)
		xfree(sst);

	return NULL;
}

int sst_add(struct sst *sst, struct sst_item *item)
{
	int cmp;
	int res;
	int pos;

	/* bloom filter */
	if (item->opt == 1)
		bloom_add(sst->bf, item->data);

	int klen = strlen(item->data);
	pos = HEADER_SIZE + sst->header.count[0] * ITEM_SIZE;
	/* swap max key */
	cmp = strcmp(item->data, sst->header.max_key);
	if (cmp > 0) { 
		memset(sst->header.max_key, 0, NESSDB_MAX_KEY_SIZE);
		memcpy(sst->header.max_key, item->data, klen);
	}

	res = pwrite(sst->fd, item, ITEM_SIZE, pos);
	if (res == -1)
		goto ERR;

	/* update header */
	sst->header.count[0]++;

	_update_header(sst);

	/* if L0 is full, to check */
	if (sst->header.count[0] >= _level_max(0, 1))
		_check_merge(sst);
	
	return 1;

ERR:
	return 0;
}

void sst_truncate(struct sst *sst)
{
	memset(&sst->header, 0, HEADER_SIZE);
	sst->willfull = 0;
}

struct sst_item *sst_in_one(struct sst *sst, int *c) 
{
	int i;
	int cur_lc;
	int pre_lc = 0;
	struct sst_item *L = NULL; 

	for (i = 0; i < MAX_LEVEL; i++) {
		cur_lc = sst->header.count[i];
		if (cur_lc > 0) {
			if (i == 0) {
				L = read_one_level(sst, i, cur_lc);
				pre_lc += cur_lc;
			} else {
				struct sst_item *pre = L;
				struct sst_item *cur = read_one_level(sst, i, cur_lc);

				L = xcalloc(cur_lc + pre_lc + 1, ITEM_SIZE);
				pre_lc = _merge_sort(sst->cpt, L, cur, cur_lc, pre, pre_lc);

				xfree(pre);
				xfree(cur);
			}
		}
	}
	*c = pre_lc;
	sst->stats->STATS_SST_MERGEONE++;

	return L;
}

#define BLOCK_SIZE (BLOCK_GAP * ITEM_SIZE)
int sst_get(struct sst *sst, struct slice *sk, struct ol_pair *pair)
{
	int cmp;
	int i = 0;
	int c = sst->header.count[i];
	struct sst_item *L = read_one_level(sst, 0, c);

	/* level 0 */
	for (i = 0; i < c; i++) {
		cmp = strcmp(sk->data, L[i].data);
		if (cmp == 0) {
			if (L[i].opt == 1) {
				pair->offset = L[i].offset;
				pair->vlen = L[i].vlen;
			}
			xfree(L);

			goto RET;
		}
	}
	xfree(L);

	for (i = 1; i < MAX_LEVEL; i++) {
		int k;
		int res;
		int blk_idx;

		if (sst->header.count[i] == 0)
			continue;

		blk_idx = block_search(sst->blk, sk, i);
		if (blk_idx < 0)
			continue;

		res = pread(sst->fd, sst->oneblk, BLOCK_SIZE, _pos_calc(i) + blk_idx * BLOCK_SIZE);
		if (res == -1)
			goto ERR;

		for (k = 0; k < BLOCK_GAP; k++) {
			cmp = strncmp(sk->data, sst->oneblk[k].data, sk->len);
			if (cmp == 0) {
				if (sst->oneblk[k].opt == 1) {
					pair->offset = sst->oneblk[k].offset;
					pair->vlen = sst->oneblk[k].vlen;
				}
				goto RET;
			}
		}
	}

RET:
	return 1;
ERR:
	return 0;
}

int sst_level_isfull(struct sst *sst, int level)
{
	return sst->header.count[level] > _level_max(level, 1<<8);
}

void sst_free(struct sst *sst)
{
	if (sst->fd > 0)
		close(sst->fd);

	bloom_free(sst->bf);
	block_free(sst->blk);
	xfree(sst->oneblk);
	xfree(sst);
}
