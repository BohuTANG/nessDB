/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/small-splittable-tree.txt
 */

#include "config.h"
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "xmalloc.h"
#include "cola.h"
#include "sorts.h"
#include "debug.h"

#define DENSITY (0.9)

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

int _level_max_size(int level)
{
	return (int)((1<<level) * L0_SIZE - 3 * ITEM_SIZE);
}

void cola_dump(struct cola *cola)
{
	int i;
	int all = 0;

	for(i = 0; i< (int)MAX_LEVEL; i++) {
		printf("\tL#%d---used#%d, max#%d\n", i, cola->header.used[i], _level_max_size(i));
		all += cola->header.used[i];
	}
	printf("\tAll used#%d\n", all);
}

void  _merge_to_next(struct cola *cola, int level) 
{
	int res;
	int lused = cola->header.used[level];
	int l_c = lused / ITEM_SIZE;

	int lnxt_used = cola->header.used[level + 1];
	int lnxt_c = lnxt_used / ITEM_SIZE;

	int lmerge_used = lused + lnxt_used;
	int lmerge_c = lmerge_used / ITEM_SIZE;

	struct cola_item *L = xcalloc(l_c + 1, ITEM_SIZE);
	struct cola_item *L_nxt = xcalloc(lnxt_c + 1, ITEM_SIZE);
	struct cola_item *L_merge = xcalloc(lmerge_c + 1, ITEM_SIZE);

	res = pread(cola->fd, L, lused, _pos_calc(level));
	if (res == -1) {
		__PANIC("read level#%d error", level);
		goto RET;
	}

	res = pread(cola->fd, L_nxt, lnxt_used, _pos_calc(level + 1));
	if (res == -1) {
		__PANIC("read level#%d error", level + 1);
		goto RET;
	}

	/* if L0, should sort it*/
	if (level == 0)
		cola_insertion_sort(L, l_c);

	lmerge_c = cola_merge_sort(L_merge, L, l_c, L_nxt, lnxt_c);
	lmerge_used = lmerge_c * ITEM_SIZE;
	res = pwrite(cola->fd, L_merge, lmerge_used, _pos_calc(level + 1));

	/* update count */
	cola->header.used[level] = 0;
	cola->header.used[level + 1] = lmerge_used;
	_update_header(cola);

RET:
	free(L);
	free(L_nxt);
	free(L_merge);
} 

void _check_merge(struct cola *cola)
{
	int i;
	int full = 0;

	int l_used;
	int l_max;
	int l_nxt_used;
	int l_nxt_max;

	/* max level check */
	i = MAX_LEVEL -1;
	l_used = cola->header.used[i];
	l_max = _level_max_size(i);

	if (l_used >= l_max)
		full++;

	for (i = MAX_LEVEL - 2; i >= 0; i--) {
		l_used = cola->header.used[i];
		l_max = _level_max_size(i);
		l_nxt_used = cola->header.used[i + 1];
		l_nxt_max = _level_max_size(i + 1);

		if (l_used >= l_max * 0.5) {
			if ((l_used + l_nxt_used) >= l_nxt_max) {
				full++;
			} else {
				_merge_to_next(cola, i);
				full--;
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

int cola_add(struct cola *cola, struct slice *sk, uint64_t offset, char opt)
{
	int cmp;
	int res;
	int pos;
	struct cola_item item;

	memset(&item, 0, ITEM_SIZE);
	memcpy(item.data, sk->data, sk->len);
	item.offset = offset;
	item.opt = opt;

	/* bloom filter */
	if (opt == 1)
		bloom_add(cola->bf, sk->data);

	/* swap max key */
	cmp = strcmp(sk->data, cola->header.max_key);
	if (cmp > 0) { 
		memset(cola->header.max_key, 0, NESSDB_MAX_KEY_SIZE);
		memcpy(cola->header.max_key, sk->data, sk->len);
	}

	/* write record */
	pos = HEADER_SIZE + cola->header.used[0];
	res = pwrite(cola->fd, &item, ITEM_SIZE, pos);
	if (res == -1)
		goto ERR;

	/* update header */
	cola->header.used[0] += ITEM_SIZE;
	_update_header(cola);

	/* if L0 is full, to check */
	if (cola->header.used[0] >= _level_max_size(0)) {
		_check_merge(cola);
	}

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
	int res;
	int l_used;
	int cur_lc;
	int pre_lc = 0;
	struct cola_item *L = NULL; 

	for (i = 0; i < MAX_LEVEL; i++) {
		l_used = cola->header.used[i];
		cur_lc = l_used / ITEM_SIZE;

		if (cur_lc > 0) {
			if (i == 0) {
				L = xcalloc(cur_lc, ITEM_SIZE);
				res = pread(cola->fd, L, l_used, _pos_calc(i));
				if (res == -1)
					__PANIC("pread error");

				cola_insertion_sort(L, cur_lc);
				
			} else {
				struct cola_item *pre = L;
				struct cola_item *cur = xcalloc(cur_lc + 1, ITEM_SIZE);

				L = xcalloc(cur_lc + pre_lc + 1, ITEM_SIZE);
				res = pread(cola->fd, cur, l_used, _pos_calc(i));
				if (res == -1)
					__PANIC("pread error");

				pre_lc = cola_merge_sort(L, cur, cur_lc, pre, pre_lc);

				free(pre);
				free(cur);
			}
		}
	}

	*c  = pre_lc;

	return L;
}

uint64_t cola_get(struct cola *cola, struct slice *sk)
{
	int i;
	int res;
	uint64_t off = 0UL;
	struct cola_item *L = NULL;

	int c = cola->header.used[0] / ITEM_SIZE;
	
	if(c > 0) {
		L = xcalloc(c, ITEM_SIZE);
		res = pread(cola->fd, L, cola->header.used[0], _pos_calc(0));
		for (i = 0; i < c; i++) {
			int cmp = strcmp(sk->data, L[i].data);
			if (cmp == 0) {
				if (L[i].opt == 1)
					off = L[i].offset;

				goto RET;
			}
		}
	}

	for (i = 1; i < MAX_LEVEL; i++) {
		int cmp;
		struct cola_item item;
		int c = cola->header.used[i] / ITEM_SIZE;
		int left = 0, right = c, mid = 0;

		if (c > 0) {
			while (left < right) {
				mid = (left + right) / 2;
				res = pread(cola->fd, &item, ITEM_SIZE, _pos_calc(i) + mid * ITEM_SIZE);
				if (res == -1)
					goto RET;

				cmp = strcmp(sk->data, item.data);
				if (cmp == 0) {
					if (item.opt == 1)
						off = item.offset;

					goto RET;
				}

				if (cmp < 0)
					right = mid;
				else 
					left = mid + 1;
			}
		}
	}

RET:
	if (L)
		free(L);

	return off;
}

void cola_free(struct cola *cola)
{
	close(cola->fd);
	bloom_free(cola->bf);
	free(cola);
}
