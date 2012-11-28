/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/small-splittable-tree.txt
 */

#include "cola.h"
#include "sorts.h"
#include "debug.h"
#include "xmalloc.h"

/* 
 * calc level's offset of file 
 */
int _pos_calc(int level)
{
	int i = 0;
	int off = HEADER_SIZE;
	
	while(i < level) {
		off += ((1<<i) * L0_SIZE);
		i++;
	}

	return off;
}

void _update_header(struct cola *cola)
{
	int res;

	res = pwrite(cola->fd, &cola->header, HEADER_SIZE, 0);
	if (res == -1)
		return;
}

int _level_max(int level, int gap)
{
	return (int)((1<<level) * L0_SIZE / ITEM_SIZE - gap);
}

void cola_dump(struct cola *cola) {
	int i;

	__DEBUG("**%06d.SST dump:", cola->fd);
	for(i = 0; i< (int)MAX_LEVEL; i++) {
		printf("\t\t-L#%d---count#%d, max-count:%d\n",
				i,
				cola->header.count[i],
				(int)_level_max(i, 0));
	}
	printf("\n");
}

struct cola_item * read_one_level(struct cola *cola, int level, int readc)
{
	int res;
	int c = cola->header.count[level];
	struct cola_item *L = xcalloc(readc + 1, ITEM_SIZE);

	if (c > 0) {
		res = pread(cola->fd, L, readc * ITEM_SIZE, _pos_calc(level) + (c - readc) * ITEM_SIZE);
		if (res == -1)
			__PANIC("read klen error");

		if (level == 0)
			cola_insertion_sort(L, readc);
	}

	return L;
}

void write_one_level(struct cola *cola, struct cola_item *L, int count, int level)
{
	int res;

	res = pwrite(cola->fd, L, count * ITEM_SIZE, _pos_calc(level));
	if (res == -1)
		__PANIC("write to one level....");
}

void  _merge_to_next(struct cola *cola, int level, int mergec) 
{
	int c2 = cola->header.count[level + 1];
	int lmerge_c = mergec + c2;
	struct cola_item *L = read_one_level(cola, level, mergec);
	struct cola_item *L_nxt = read_one_level(cola, level + 1, c2);
	struct cola_item *L_merge = xcalloc(lmerge_c + 1, ITEM_SIZE);

	lmerge_c = cola_merge_sort(cola->cpt, L_merge, L, mergec, L_nxt, c2);
	write_one_level(cola, L_merge, lmerge_c, level + 1);

	/* update count */
	cola->header.count[level] -= mergec;
	cola->header.count[level + 1] = lmerge_c;

	_update_header(cola);

	free(L);
	free(L_nxt);
	free(L_merge);

	cola->stats->STATS_LEVEL_MERGES++;
} 

void _check_merge(struct cola *cola)
{
	int i;
	int c;
	int max;
	int nxt_c = 0;
	int nxt_max = 0;
	int full = 0;

	for (i = MAX_LEVEL - 1; i >= 0; i--) {
		c = cola->header.count[i];
		max = _level_max(i, 3);

		/* bottom level */
		if (i == (MAX_LEVEL - 1)) {
			if (c >= max)
				full++;
			continue;
		} else {
			nxt_c = cola->header.count[i + 1];
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
				_merge_to_next(cola, i, c);
			} else {
				diff = nxt_max - nxt_c;
				if (diff > 0)
					_merge_to_next(cola, i, diff);
			}
		}
	} 

	if (full >= (MAX_LEVEL - 1)) {
		__DEBUG("--all levels[%d] is full#%d, need to be scryed...", 
				MAX_LEVEL - 1,  
				full);

		cola->willfull = 1;
		cola_dump(cola);
	}
}

void _build_block(struct cola *cola)
{
	int i;

	for (i = 0; i < MAX_LEVEL; i++) {
		int c;
		struct cola_item *L;

		c = cola->header.count[i];
		L = read_one_level(cola, i, c);
		if (c > 0)
			block_build(cola->blk, L, c, i); 
		free(L);
	}
}

#define BLOCK0_COUNT ((L0_SIZE/ITEM_SIZE)/BLOCK_GAP + 1)
struct cola *cola_new(const char *file, struct compact *cpt, struct stats *stats)
{
	int res;
	struct cola *cola = xcalloc(1, sizeof(struct cola));

	cola->oneblk = xcalloc(BLOCK_GAP, ITEM_SIZE);
	cola->blk= block_new(BLOCK0_COUNT);

	cola->fd = n_open(file, N_OPEN_FLAGS, 0644);
	if (cola->fd > 0) {
		res = pread(cola->fd, &cola->header, HEADER_SIZE, 0);
		if (res == -1)
			goto ERR;
		_build_block(cola);
	} else
		cola->fd = n_open(file, N_CREAT_FLAGS, 0644);

	cola->stats = stats;
	cola->bf = bloom_new(cola->header.bitset);
	if (cpt != NULL)
		cola->cpt = cpt;

	return cola;

ERR:
	if (cola)
		free(cola);

	return NULL;
}

STATUS cola_add(struct cola *cola, struct cola_item *item)
{
	int cmp;
	int res;
	int pos;

	/* bloom filter */
	if (item->opt == 1)
		bloom_add(cola->bf, item->data);

	int klen = strlen(item->data);
	pos = HEADER_SIZE + cola->header.count[0] * ITEM_SIZE;
	/* swap max key */
	cmp = strcmp(item->data, cola->header.max_key);
	if (cmp > 0) { 
		memset(cola->header.max_key, 0, NESSDB_MAX_KEY_SIZE);
		memcpy(cola->header.max_key, item->data, klen);
	}

	res = pwrite(cola->fd, item, ITEM_SIZE, pos);
	if (res == -1)
		goto ERR;

	/* update header */
	cola->header.count[0]++;

	_update_header(cola);

	/* if L0 is full, to check */
	if (cola->header.count[0] >= _level_max(0, 1))
		_check_merge(cola);
	
	return nOK;

ERR:
	return nERR;
}

void cola_truncate(struct cola *cola)
{
	memset(&cola->header, 0, HEADER_SIZE);
	cola->willfull = 0;
}

struct cola_item *cola_in_one(struct cola *cola, int *c) 
{
	int i;
	int cur_lc;
	int pre_lc = 0;
	struct cola_item *L = NULL; 

	for (i = 0; i < MAX_LEVEL; i++) {
		cur_lc = cola->header.count[i];
		if (cur_lc > 0) {
			if (i == 0) {
				L = read_one_level(cola, i, cur_lc);
				pre_lc += cur_lc;
			} else {
				struct cola_item *pre = L;
				struct cola_item *cur = read_one_level(cola, i, cur_lc);

				L = xcalloc(cur_lc + pre_lc + 1, ITEM_SIZE);
				pre_lc = cola_merge_sort(cola->cpt, L, cur, cur_lc, pre, pre_lc);

				free(pre);
				free(cur);
			}
		}
	}
	*c = pre_lc;
	cola->stats->STATS_SST_MERGEONE++;

	return L;
}

#define BLOCK_SIZE (BLOCK_GAP * ITEM_SIZE)
STATUS cola_get(struct cola *cola, struct slice *sk, struct ol_pair *pair)
{
	int cmp;
	int i = 0;
	int c = cola->header.count[i];
	struct cola_item *L = read_one_level(cola, 0, c);

	/* level 0 */
	for (i = 0; i < c; i++) {
		cmp = strcmp(sk->data, L[i].data);
		if (cmp == 0) {
			if (L[i].opt == 1) {
				pair->offset = L[i].offset;
				pair->vlen = L[i].vlen;
			}
			free(L);

			goto RET;
		}
	}
	free(L);

	for (i = 1; i < MAX_LEVEL; i++) {
		int k;
		int res;
		int blk_idx;

		if (cola->header.count[i] == 0)
			continue;

		blk_idx = block_search(cola->blk, sk->data, i);
		if (blk_idx < 0)
			continue;

		res = pread(cola->fd, cola->oneblk, BLOCK_SIZE, _pos_calc(i) + blk_idx * BLOCK_SIZE);
		if (res == -1)
			goto ERR;

		for (k = 0; k < BLOCK_GAP; k++) {
			cmp = strcmp(sk->data, cola->oneblk[k].data);
			if (cmp == 0) {
				if (cola->oneblk[k].opt == 1) {
					pair->offset = cola->oneblk[k].offset;
					pair->vlen = cola->oneblk[k].vlen;
				}
				goto RET;
			}
		}
	}

RET:
	return nOK;
ERR:
	return nERR;
}

void cola_free(struct cola *cola)
{
	if (cola->fd > 0)
		close(cola->fd);

	bloom_free(cola->bf);
	block_free(cola->blk);
	free(cola->oneblk);
	free(cola);
}
