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
 *   * Neither the name of Redis nor the names of its contributors may be used
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
#include "llru.h"

#define MAXHITS (3)
#define PRIME	(16785407)
#define RATIO	(0.618)

static struct ht _ht;
static struct level *_level_old;
static struct level *_level_new;
static int	_buffer=0;
void llru_init(size_t buffer_size)
{
	if(buffer_size>1024)
		_buffer=1;

	ht_init(&_ht,PRIME,STRING);

	_level_old=calloc(1,sizeof(struct level));
	_level_new=calloc(1,sizeof(struct level));
	
	size_t size_n=buffer_size*RATIO;
	size_t size_o=buffer_size-size_n;

	_level_old->count=0;
	_level_old->allow_size=size_o;
	_level_old->used_size=0;
	_level_old->first=NULL;
	_level_old->last=NULL;
	_level_old->ht=&_ht;
	
	_level_new->count=0;
	_level_new->allow_size=size_n;
	_level_new->used_size=0;
	_level_new->first=NULL;
	_level_new->last=NULL;
	_level_new->ht=&_ht;
}

static void llru_set_node(struct level_node *n)
{
	if(n!=NULL){
		if(n->hits==-1){
			if(_level_new->used_size>_level_new->allow_size){
				struct level_node *new_last_node=_level_new->last;
				level_remove_link(_level_new,new_last_node);
				level_set_head(_level_old,new_last_node);
			}
			level_remove_link(_level_new,n);
			level_set_head(_level_new,n);
		}else{

			if(_level_old->used_size>_level_old->allow_size)
				level_free_last(_level_old);

			n->hits++;
			level_remove_link(_level_old,n);
			if(n->hits>MAXHITS){
				level_set_head(_level_new,n);
				n->hits=-1;
			}else
				level_set_head(_level_old,n);
		}
	}
}


void llru_set(const char *k,void *v,int k_len,int v_len)
{
	if(_buffer==0)
		return;
	struct level_node *n=ht_get(&_ht,(void*)k);
	if(n==NULL){
		_level_old->used_size+=(k_len+v_len);
		_level_old->count++;

		n=calloc(1,sizeof(struct level_node));
		n->key=(char*)k;
		n->value=v;
		n->size=(k_len+v_len);
		n->hits=1;
		n->pre=NULL;
		n->nxt=NULL;
	}
	llru_set_node(n);
	ht_set(&_ht,(void*)k,(void*)n);
}


void* llru_get(const char *k)
{
	if(_buffer==0)
		return NULL;

	struct level_node *n=ht_get(&_ht,(void*)k);
	if(n!=NULL){
		llru_set_node(n);
		return n->value;
	}
	return NULL;
}

void llru_remove(const char* k)
{
	if(_buffer==0)
		return;

	struct level_node *n=ht_get(&_ht,(void*)k);
	if(n!=NULL){

		ht_remove(&_ht,(void*)k);
		if(n->hits==-1)
			level_free_node(_level_new,n);
		else
			level_free_node(_level_old,n);
	}
}

void llru_info(struct llru_info *llru_info)
{
	llru_info->nl_count=_level_new->count;
	llru_info->ol_count=_level_old->count;

	llru_info->nl_used=_level_new->used_size;
	llru_info->ol_used=_level_old->used_size;

	llru_info->nl_allowsize=_level_new->allow_size;
	llru_info->ol_allowsize=_level_old->allow_size;
}

void llru_free()
{
	ht_free(&_ht);
}
