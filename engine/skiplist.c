/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "skiplist.h"
#include "config.h"
#include "debug.h"
#include "xmalloc.h"

#define cmp_lt(a, b) (strcmp(a, b) < 0)
#define cmp_eq(a, b) (strcmp(a, b) == 0)

#define NIL list->hdr

struct pool {
	struct pool *next;
	char *ptr;
	unsigned int rem;
};

struct pool *_pool_new()
{
	unsigned int p_size = 8092 - sizeof(struct pool);
	struct pool *pool = xcalloc(1, sizeof(struct pool) + p_size);

	pool->ptr = (char*)(pool + 1);
	pool->rem = p_size;
	
	return pool;
}

void _pool_destroy(struct pool *pool)
{
	while (pool->next != NULL) {
		struct pool *next = pool->next;
		free (pool);
		pool = next;
	}
}

void *_pool_alloc(struct skiplist *list, size_t size)
{
	struct pool *pool;
	void *ptr;

	pool = list->pool;
	if (pool->rem < size) {
		pool = _pool_new();
		pool->next = list->pool;
		list->pool = pool;
	}

	ptr = pool->ptr;
	pool->ptr += size;
	pool->rem -= size;

	return ptr;
}

struct skiplist *skiplist_new(size_t size)
{
	int i;
	struct skiplist *list = xcalloc(1, sizeof(struct skiplist));

	list->hdr = xmalloc(sizeof(struct skipnode) + MAXLEVEL*sizeof(struct skipnode *));

	for (i = 0; i <= MAXLEVEL; i++)
		list->hdr->forward[i] = NIL;

	list->size = size;
	list->pool = (struct pool *) list->pool_embedded;

	return list;
}

void skiplist_free(struct skiplist *list)
{
	_pool_destroy(list->pool);
	free(list->hdr);
	free(list);
}

int skiplist_notfull(struct skiplist *list)
{
	if (list->count < list->size)
		return 1;

	return 0;
}

int skiplist_insert(struct skiplist *list, struct cola_item *itm) 
{
	int i = 0, new_level = 0;
	struct skipnode *update[MAXLEVEL+1];
	struct skipnode *x;

	if (!skiplist_notfull(list))
		return 0;

	x = list->hdr;
	for (i = list->level; i >= 0; i--) {
		while (x->forward[i] != NIL 
				&& cmp_lt(x->forward[i]->itm.data, itm->data))
			x = x->forward[i];
		update[i] = x;
	}

	x = x->forward[0];
	if (x != NIL && cmp_eq(x->itm.data, itm->data)) {
		memcpy(&x->itm, itm, ITEM_SIZE);
		return 1;
	}

	for (new_level = 0; rand() < RAND_MAX/2 && new_level < MAXLEVEL; new_level++);

	if (new_level > list->level) {
		for (i = list->level + 1; i <= new_level; i++)
			update[i] = NIL;

		list->level = new_level;
	}

	if ((x =_pool_alloc(list,sizeof(struct skipnode) + new_level*sizeof(struct skipnode *))) == 0)
		__PANIC("Pool alloc error, maybe less memory");

	memcpy(&x->itm, itm, ITEM_SIZE);

	for (i = 0; i <= new_level; i++) {
		x->forward[i] = update[i]->forward[i];
		update[i]->forward[i] = x;
	}
	list->count++;

	return 1;
}

struct skipnode *skiplist_lookup(struct skiplist *list, char* data) 
{
	int i;
	struct skipnode *x = list->hdr;

	for (i = list->level; i >= 0; i--) {
		while (x->forward[i] != NIL 
				&& cmp_lt(x->forward[i]->itm.data, data))
			x = x->forward[i];
	}
	x = x->forward[0];
	if (x != NIL && cmp_eq(x->itm.data, data)) 
		return (x);

	return NULL;
}

