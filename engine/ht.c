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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ht.h"


static size_t find_slot(struct ht *ht, void *node)
{
	return ht->hashfunc(node) % ht->cap;
}

void ht_init(struct ht *ht, size_t cap)
{
	ht->size = 0;
	ht->cap = cap;
	ht->nodes = calloc(cap, sizeof(struct ht_node*));
}

void ht_set(struct ht *ht, struct ht_node *node)
{
	size_t slot;

	slot = find_slot(ht, node);
	node->next = ht->nodes[slot];
	ht->nodes[slot] = node;
	ht->size++;
}

struct ht_node* ht_get(struct ht *ht, void * k)
{
	size_t slot;
	struct ht_node *node;

	slot = find_slot(ht, k);
	node = ht->nodes[slot];
	while (node) {
		if (ht->cmpfunc(k, node) == 0)
			return node;
		node = node->next;
	}
	return NULL;
}

struct ht_node * ht_remove(struct ht *ht, void *k)
{
	size_t slot;
	struct ht_node *node, *prev=NULL;
	
	slot = find_slot(ht, k);
	node = ht->nodes[slot];
	while (node) {
		if( ht->cmpfunc(k, node) == 0) {
			if (prev != NULL)
				prev->next = node->next;
			else
				ht->nodes[slot] = node->next;
			ht->size--;
			return node;
		}
		prev = node;
		node = node->next;	
	}
	return NULL;
}



void ht_free(struct ht *ht)
{
	free(ht->nodes);
}

