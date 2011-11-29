/* Level lru cache: inlcude new-level and old-level two levels
 * If one's hits >MAX,it will be moved to new-level
 * It's a real non-dirty shit,and likely atomic energy level transition
 *
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
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
 *   * Neither the name of nessDB nor the names of its contributors may be used
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
#include <string.h>
#include <stdlib.h>
#include "llru.h"

#define MAXHITS (3)
#define PRIME	(16785407)
#define RATIO	(0.618)

/*TODO:hashfunc cmpfunc impl*/

size_t _ht_hashfunc(void *val)
{
	unsigned int hash = 5318;
	unsigned int c;

	if(!val)
		return 0;

	while ((c = *(char*)val++))
		hash = ((hash << 5) + hash) + (unsigned int)c;  /* hash * 33 + c */

	return (size_t) hash;
}

int _ht_cmpfunc(void *a, void *b)
{
	return strcmp((const char*)a, (const char*)b);
}

struct llru *llru_new(size_t buffer_size)
{
	size_t size_n=buffer_size*RATIO;
	size_t size_o=buffer_size-size_n;

	struct ht *ht = malloc(sizeof(struct ht));
	struct level *level_old = level_creat();
	struct level *level_new = level_creat();

	struct llru *lru = malloc(sizeof(struct llru));

	if (buffer_size > 1024)
		lru->buffer = 1;


	ht_init(ht,PRIME);
	ht->hashfunc = _ht_hashfunc;
	ht->cmpfunc = _ht_cmpfunc;
	lru->ht = ht;

	level_old->count = 0;
	level_old->allow_size = size_o;
	level_old->used_size = 0;
	level_old->first = NULL;
	level_old->last = NULL;
	lru->level_old = level_old;
	
	level_new->count = 0;
	level_new->allow_size = size_n;
	level_new->used_size = 0;
	level_new->first = NULL;
	level_new->last = NULL;
	lru->level_new = level_new;

	return lru;
}

void _llru_set_node(struct llru *lru, struct level_node *n)
{
	if( n != NULL) {
		if (n->hits == -1) {
			if (lru->level_new->used_size > lru->level_new->allow_size) {
				struct level_node *new_last_node = lru->level_new->last;
				level_remove_link(lru->level_new, new_last_node);
				level_set_head(lru->level_old, new_last_node);
			}
			level_remove_link(lru->level_new, n);
			level_set_head(lru->level_new, n);
		} else {
			if (lru->level_old->used_size > lru->level_old->allow_size)
				level_free_last(lru->level_old);

			n->hits++;
			level_remove_link(lru->level_old, n);
			if (n->hits > MAXHITS) {
				level_set_head(lru->level_new, n);
				n->hits = -1;
				lru->level_old->used_size -= n->size;
				lru->level_new->used_size += n->size;
			} else
				level_set_head(lru->level_old,n);
		}
	}
}


void llru_set(struct llru *lru, void *k, void *v, size_t size)
{
	if (lru->buffer == 0)
		return;

	struct level_node *n = ht_get(lru->ht, k);
	if (n == NULL) {
		lru->level_old->used_size += size;
		lru->level_old->count++;

		n = calloc(1, sizeof(struct level_node));
		n->key = k;
		n->value = v;
		n->size = size;
		n->hits = 1;
		n->pre = NULL;
		n->nxt = NULL;
	}

	_llru_set_node(lru, n);
	ht_set(lru->ht, k, v);
}


void* llru_get(struct llru *lru, void *k)
{
	if (lru->buffer == 0)
		return NULL;

	struct level_node *n = ht_get(lru->ht, k);
	if (n != NULL) {
		_llru_set_node(lru, n);
		return n->value;
	}

	return NULL;
}

void llru_remove(struct llru *lru, void *k)
{
	if (lru->buffer == 0)
		return;

	struct level_node *n = ht_get(lru->ht, k);
	if (n != NULL) {
		ht_remove(lru->ht, k);
		if (n->hits == -1)
			level_free_node(lru->level_new, n);
		else
			level_free_node(lru->level_old, n);
	}
	/*TODO: free*/
}


void llru_free(struct llru *lru)
{
	ht_free(lru->ht);
	/*TODO:level free*/
}
