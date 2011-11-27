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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "llru.h"

#define MAXHITS (3)
#define PRIME	(16785407)
#define RATIO	(0.618)

void llru_init(llru_t *self, size_t buffer_size)
{
	size_t size_n=buffer_size*RATIO;
	size_t size_o=buffer_size-size_n;
	memset(self, 0, sizeof(llru_t));
	ht_init(&self->ht,PRIME,STRING);
	level_init(&self->level_old, size_o);
	level_init(&self->level_new, size_n);
}

static void llru_set_node(llru_t *self, struct level_node *n)
{
	if(n!=NULL){
		if(n->hits==-1){
			if(self->level_new.used_size > self->level_new.allow_size){
				struct level_node *new_last_node = self->level_new.last;
				level_remove_link(&self->level_new,new_last_node);
				level_set_head(&self->level_old,new_last_node);
			}
			level_remove_link(&self->level_new, n);
			level_set_head(&self->level_new, n);
		}else{

			if(self->level_old.used_size > self->level_old.allow_size)
				level_free_last(&self->level_old);

			n->hits++;
			level_remove_link(&self->level_old, n);
			if(n->hits>MAXHITS){
				level_set_head(&self->level_new, n);
				n->hits=-1;
				self->level_old.used_size -= n->size;
				self->level_new.used_size += n->size;
			}else
				level_set_head(&self->level_old, n);
		}
	}
}


void llru_set(llru_t *self, const char *k, void *v, size_t k_len, size_t v_len)
{
	int size=(k_len+v_len);
	if(self->buffer==0)
		return;
	struct level_node *n=ht_get(&self->ht,(void*)k);
	if(n==NULL){
		self->level_old.used_size += size;
		self->level_old.count++;

		n=calloc(1,sizeof(struct level_node));
		n->key=(char*)k;
		n->value=v;
		n->size=size;
		n->hits=1;
		n->pre=NULL;
		n->nxt=NULL;
	}
	llru_set_node(self, n);
	ht_set(&self->ht,(void*)k,(void*)n);
}


void* llru_get(llru_t *self, const char *k)
{
	if(self->buffer==0)
		return NULL;

	struct level_node *n=ht_get(&self->ht,(void*)k);
	if(n!=NULL){
		llru_set_node(self, n);
		return n->value;
	}
	return NULL;
}

void llru_remove(llru_t *self, const char* k)
{
	if(self->buffer==0)
		return;

	struct level_node *n=ht_get(&self->ht,(void*)k);
	if(n!=NULL){

		ht_remove(&self->ht,(void*)k);
		if(n->hits==-1)
			level_free_node(&self->level_new,n);
		else
			level_free_node(&self->level_old,n);
	}
}

void llru_info(llru_t *self, struct llru_info *llru_info)
{
	llru_info->nl_count = self->level_new.count;
	llru_info->ol_count = self->level_old.count;

	llru_info->nl_used = self->level_new.used_size;
	llru_info->ol_used = self->level_old.used_size;

	llru_info->nl_allowsize = self->level_new.allow_size;
	llru_info->ol_allowsize = self->level_old.allow_size;
}

void llru_free(llru_t *self)
{
	ht_free(&self->ht);
}
