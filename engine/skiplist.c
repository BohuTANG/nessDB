 /* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of lsmtree nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "skiplist.h"
#include "debug.h"

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
	unsigned int p_size = 1024*512 - sizeof(struct pool);
	struct pool *pool = malloc(sizeof(struct pool) + p_size);

	memset(pool, 0, p_size);
	pool->next = NULL;
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

	struct skiplist *list = malloc(sizeof(struct skiplist));
	list->hdr = malloc(sizeof(struct skipnode) + MAXLEVEL*sizeof(struct skipnode *));

	for (i = 0; i <= MAXLEVEL; i++)
		list->hdr->forward[i] = NIL;

	list->level = 0;
	list->size = size;
	list->count = 0;
	list->pool = (struct pool *) list->pool_embedded;
	list->pool->rem = 0;
	list->pool->next = NULL;
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

int skiplist_insert(struct skiplist *list, struct slice *sk, uint64_t val, OPT opt) 
{
	int i, new_level;
	struct skipnode *update[MAXLEVEL+1];
	struct skipnode *x;

	char *key = sk->data;

	if (!skiplist_notfull(list))
		return 0;

	x = list->hdr;
	for (i = list->level; i >= 0; i--) {
		while (x->forward[i] != NIL 
				&& cmp_lt(x->forward[i]->key, key))
			x = x->forward[i];
		update[i] = x;
	}

	x = x->forward[0];
	if (x != NIL && cmp_eq(x->key, key)) {
		x->val = val;
		x->opt = opt;
		return(1);
	}

	for (new_level = 0; rand() < RAND_MAX/2 && new_level < MAXLEVEL; new_level++);

	if (new_level > list->level) {
		for (i = list->level + 1; i <= new_level; i++)
			update[i] = NIL;

		list->level = new_level;
	}

	if ((x =_pool_alloc(list,sizeof(struct skipnode) + new_level*sizeof(struct skipnode *))) == 0)
		__DEBUG("%s", "ERROR: Alloc Memory *ERROR*");

	memcpy(x->key, key, sk->len);
	x->val = val;
	x->opt = opt;

	for (i = 0; i <= new_level; i++) {
		x->forward[i] = update[i]->forward[i];
		update[i]->forward[i] = x;
	}
	list->count++;

	return(1);
}

void skiplist_delete(struct skiplist *list, char* data) 
{
	int i;
	struct skipnode *update[MAXLEVEL+1], *x;

	x = list->hdr;
	for (i = list->level; i >= 0; i--) {
		while (x->forward[i] != NIL 
				&& cmp_lt(x->forward[i]->key, data))
			x = x->forward[i];
		update[i] = x;
	}
	x = x->forward[0];
	if (x == NIL || !cmp_eq(x->key, data))
		return;

	for (i = 0; i <= list->level; i++) {
		if (update[i]->forward[i] != x)
			break;
		update[i]->forward[i] = x->forward[i];
	}
	free (x);

	while ((list->level > 0)
			&& (list->hdr->forward[list->level] == NIL))
		list->level--;
}

struct skipnode *skiplist_lookup(struct skiplist *list, struct slice *sk) 
{
	int i;
	char *data = sk->data;
	struct skipnode *x = list->hdr;
	for (i = list->level; i >= 0; i--) {
		while (x->forward[i] != NIL 
				&& cmp_lt(x->forward[i]->key, data))
			x = x->forward[i];
	}
	x = x->forward[0];
	if (x != NIL && cmp_eq(x->key, data)) 
		return (x);

	return NULL;
}


void skiplist_dump(struct skiplist *list)
{
	int i;
	struct skipnode *x = list->hdr->forward[0];

	printf("--skiplist dump:level<%d>,size:<%d>,count:<%d>\n",
			list->level,
			(int)list->size,
			(int)list->count);

	for (i=0; i<list->count; i++) {
		printf("	[%d]key:<%s>;val<%llu>;opt<%s>\n", i, x->key, x->val, x->opt == ADD?"ADD":"DEL");
		x = x->forward[0];
	}
}
