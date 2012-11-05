/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/small-splittable-tree.txt
 */

#include "config.h"
#include "xmalloc.h"
#include "cola.h"
#include "sorts.h"
#include "debug.h"

/* calc level's offset of file */
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

void cola_dump(struct cola *cola)
{
	int i;

	__DEBUG("**%06d.SST dump:", cola->fd);
	for(i = 0; i< (int)MAX_LEVEL; i++) {
		printf("\t\t-L#%d---count#%d, max-count:%d\n"
				, i
				, cola->header.count[i]
				, (int)_level_max(i, 0));
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

	lmerge_c = cola_merge_sort(L_merge, L, mergec, L_nxt, c2);
	write_one_level(cola, L_merge, lmerge_c, level + 1);

	/* update count */
	cola->header.count[level] -= mergec;
	cola->header.count[level + 1] = lmerge_c;

	_update_header(cola);

	free(L);
	free(L_nxt);
	free(L_merge);
} 

void _check_merge(struct cola *cola)
{
	int i;
	int l_c;
	int l_max;
	int l_nxt_c;
	int l_nxt_max;
	int full = 0;

	i = MAX_LEVEL - 1;
	l_c = cola->header.count[i];
	l_max = _level_max(i, 3);
	if (l_c >= l_max)
		full++;

	for (i = MAX_LEVEL - 2; i >= 0; i--) {
		l_c = cola->header.count[i];
		l_max = _level_max(i, 3);

		l_nxt_c = cola->header.count[i + 1];
		l_nxt_max = _level_max(i + 1, 3);

		if (l_nxt_c >= l_nxt_max) {
			full++;
			continue;
		}

		if (l_c >= l_max) {
			int diff = l_nxt_max - (l_c + l_nxt_c);

			/* merge full level to next level */
			if (diff >= 0) {
				_merge_to_next(cola, i, l_c);
			} else {
				diff = l_nxt_max - l_nxt_c;
				_merge_to_next(cola, i, diff);
			}
		}
	} 

	if (full >= (MAX_LEVEL - 1))
		cola->willfull = 1;
}

struct cola *cola_new(const char *file)
{
	int res;
	struct cola *cola = xcalloc(1, sizeof(struct cola));

	cola->fd = n_open(file, N_OPEN_FLAGS, 0644);
	if (cola->fd > 0) {
		res = pread(cola->fd, &cola->header, HEADER_SIZE, 0);
		if (res == -1)
			goto ERR;
	} else
		cola->fd = n_open(file, N_CREAT_FLAGS, 0644);

	cola->bf = bloom_new(cola->header.bitset);

	return cola;

ERR:
	if (cola)
		free(cola);

	return NULL;
}

int cola_add(struct cola *cola, struct cola_item *item)
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
	
	return 1;

ERR:
	return 0;
}

void cola_truncate(struct cola *cola)
{
	memset(&cola->header, 0, HEADER_SIZE);
	cola->willfull = 0;

	//todo truncate file?
}

struct cola_item *cola_in_one(struct cola *cola, int *c) 
{
	int i;
	int cur_lc;
	int pre_lc = cola->header.count[0];
	struct cola_item *L = NULL; 

	for (i = 0; i < MAX_LEVEL; i++) {
		cur_lc = cola->header.count[i];
		if (cur_lc > 0) {
			if (i == 0) {
				L = read_one_level(cola, i, cur_lc);
			} else {
				struct cola_item *pre = L;
				struct cola_item *cur = read_one_level(cola, i, cur_lc);

				L = xcalloc(cur_lc + pre_lc + 1, ITEM_SIZE);
				pre_lc = cola_merge_sort(L, cur, cur_lc, pre, pre_lc);

				free(pre);
				free(cur);
			}
		}
	}

	*c  = pre_lc;

	return L;
}

int  cola_get(struct cola *cola, struct slice *sk, struct ol_pair *pair)
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
			return 1;
		}
	}
	free(L);

	for (i = 1; i < MAX_LEVEL; i++) {
		int res;
		struct cola_item itm;
		int left = 0;
		int mid;
		int right = cola->header.count[i];

		while (left < right) {
			mid = (left + right) / 2;
			res = pread(cola->fd, &itm, sizeof(itm), _pos_calc(i) + mid * sizeof(itm));
			if (res == -1)
				goto RET;

			cmp = strcmp(sk->data, itm.data);
			if (cmp == 0) {
				if (itm.opt == 1) {
					pair->offset = itm.offset;
					pair->vlen = itm.vlen;
				}

				return 1;
			}

			if (cmp < 0)
				right = mid;
			else 
				left = mid + 1;
		}
	}

RET:
	return 0;
}

void cola_free(struct cola *cola)
{
	if (cola->fd > 0)
		close(cola->fd);

	bloom_free(cola->bf);
	free(cola);
}
