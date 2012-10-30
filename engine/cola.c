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

struct cola_item * read_one_level(struct cola *cola, int level)
{
	int i;
	int res;
	int c = cola->header.count[level];
	struct cola_item *L = xcalloc(c + 1, ITEM_SIZE);

	int used = cola->header.used[level];
	struct buffer *buffer = buffer_new(used + 1);

	res = pread(cola->fd, buffer->buf, used, _pos_calc(level));
	if (res == -1)
		__PANIC("read klen error");

	for (i = 0; i < c; i++) {
		int klen = buffer_getuint(buffer);
		buffer_getnstr(buffer, L[i].data, klen);
		L[i].offset = buffer_getulong(buffer);
		L[i].vlen = buffer_getuint(buffer);
		L[i].opt= buffer_getc(buffer);
	}

	buffer_free(buffer);

	if (level == 0)
		cola_insertion_sort(L, c);

	return L;
}

int write_one_level(struct cola *cola, struct cola_item *L, int count, int level)
{
	int i;
	int res;
	int used = 0;
	char *line = NULL;
	struct buffer *buf = buffer_new(1024 * 1024 * 32);

	for (i = 0; i < count; i++) {
		int len = strlen(L[i].data);

		/* klen | key | offset | vlen | opt */
		buffer_putint(buf, len);
		buffer_putnstr(buf, L[i].data, len);
		buffer_putlong(buf, L[i].offset);
		buffer_putint(buf, L[i].vlen);
		buffer_putc(buf, L[i].opt);
	}

	used = buf->NUL;
	line = buffer_detach(buf);
	res = pwrite(cola->fd, line, used, _pos_calc(level));
	if (res == -1)
		__PANIC("write to one level....");

	buffer_free(buf);

	return used;
}

void  _merge_to_next(struct cola *cola, int level) 
{
	int used = 0;
	int l_c = cola->header.count[level];
	int lnxt_c = cola->header.count[level + 1];
	int lmerge_c = l_c + lnxt_c;

	struct cola_item *L = read_one_level(cola, level);
	struct cola_item *L_nxt = read_one_level(cola, level + 1);
	struct cola_item *L_merge = xcalloc(lmerge_c + 1, ITEM_SIZE);

	lmerge_c = cola_merge_sort(L_merge, L, l_c, L_nxt, lnxt_c);
	used = write_one_level(cola, L_merge, lmerge_c, level + 1);

	/* update count */
	cola->header.used[level] = 0;
	cola->header.count[level] = 0;
	cola->header.used[level + 1] = used;
	cola->header.count[level + 1] = lmerge_c;
	_update_header(cola);

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
				//full--;
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
	cola->buf = buffer_new(1024*1024);

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
	int blen;
	char *line;

	/* bloom filter */
	if (item->opt == 1)
		bloom_add(cola->bf, item->data);

	int klen = strlen(item->data);
	pos = HEADER_SIZE + cola->header.used[0];
	/* swap max key */
	cmp = strcmp(item->data, cola->header.max_key);
	if (cmp > 0) { 
		memset(cola->header.max_key, 0, NESSDB_MAX_KEY_SIZE);
		memcpy(cola->header.max_key, item->data, klen);
	}

	buffer_putint(cola->buf, klen);
	buffer_putnstr(cola->buf, item->data, klen);
	buffer_putlong(cola->buf, item->offset);
	buffer_putint(cola->buf, item->vlen);
	buffer_putc(cola->buf, item->opt);

	blen = cola->buf->NUL;
	line = buffer_detach(cola->buf);

	res = pwrite(cola->fd, line, blen, pos);
	if (res == -1)
		goto ERR;

	/* update header */
	cola->header.used[0] += blen;
	cola->header.count[0]++;
	_update_header(cola);

	/* if L0 is full, to check */
	if (cola->header.used[0] >= _level_max_size(0) * 0.75) {
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
	int cur_lc;
	int pre_lc = 0;
	struct cola_item *L = NULL; 

	for (i = 0; i < MAX_LEVEL; i++) {
		cur_lc = cola->header.count[i];
		if (cur_lc > 0) {
			if (i == 0) {
				L = read_one_level(cola, i);
				
			} else {
				struct cola_item *pre = L;
				struct cola_item *cur = read_one_level(cola, i);

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
