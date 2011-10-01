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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>
#include "bitwise.h"
#include "storage.h"
#include "db.h"
#include "llru.h"

#define IDX_PRIME	(16785407)

static struct btree 	_btree;
static struct bloom	_bloom;


/*Sequential read,and mark keys in BloomFilter
* This is very important to preheat datas.
*/
static void db_loadbloom()
{
	int i,l,super_size=sizeof(struct btree_super);
	uint64_t alloc=_btree.alloc-super_size,offset;
	int newsize=(sizeof(struct btree_table));
	lseek64(_btree.fd,super_size, SEEK_SET);
	while(alloc>0){
		struct btree_table *table=malloc(newsize);
		int r=read(_btree.fd,table, newsize) ;
		if(table->size>0){
			for(l=0;l<table->size;l++){
				if(get_H(offset)==0)
					bloom_add(&_bloom,table->items[l].sha1);
			}
		}
		free(table);
		alloc-=newsize;
	}
}

void db_init(int bufferpool_size)
{
	btree_init(&_btree);
	llru_init(bufferpool_size);
	bloom_init(&_bloom,IDX_PRIME);
	db_loadbloom();	
}


int db_add(char* key,char* value)
{
	uint64_t off=btree_insert(&_btree,(const uint8_t*)key,(const void*)value,strlen(value));
	if(off==0)
		return (0);
	bloom_add(&_bloom,key);
	return (1);
}


void *db_get(char* key)
{
	int b=bloom_get(&_bloom,(const char*)key);
	if(b!=0)
		return NULL;

	void *v=llru_get((const char*)key);
	if(v==NULL){
		v=btree_get(&_btree,key);
		if(v==NULL)
			return NULL;

		char *k_tmp=strdup(key);
		char *v_tmp=strdup((char*)v);
		llru_set(k_tmp,v_tmp,strlen(k_tmp),strlen(v_tmp));
		return v;
	}else{
		return strdup((char*)v);
	}
}

void db_remove(char* key)
{
	btree_delete(&_btree,key);
	llru_remove(key);
}

void db_destroy()
{
	llru_free();
	btree_close(&_btree);
}
