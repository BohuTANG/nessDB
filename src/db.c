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

#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include "bitwise.h"
#include "storage.h"
#include "llru.h"
#include "hashes.h"
#include "db.h"

/*primes are:
 3UL, 5UL, 7UL, 11UL, 13UL, 17UL, 19UL, 23UL, 29UL, 31UL, 37UL,41UL, 43UL, 47UL,
 53UL, 97UL, 193UL, 389UL, 769UL, 1543UL, 3079UL, 6151UL, 12289UL,
 24593UL, 49157UL, 98317UL, 196613UL, 393241UL, 786433UL, 1572869UL,
 3145739UL, 6291469UL, 12582917UL, 25165843UL, 50331653UL,
 100663319UL, 201326611UL, 402653189UL, 805306457UL, 1610612741UL,
 3221225473UL, 4294967291UL
*/
#define DB_SLOT		(13)
#define DB_PREFIX 	"ndbs/ness"
#define IDX_PRIME	(16785407)
static struct btree 	_btrees[DB_SLOT];
static struct bloom	_bloom;

static pthread_t	_bgsync;
void *bgsync_func();

void *bgsync_func()
{
	int i;
	while(1){
/*
		if(iter>10000){
			iter=0;
			usleep(1000);
		}else
			iter++;
*/
		sleep(5);
		for(i=0;i<DB_SLOT;i++){
			fdatasync(_btrees[i].fd);
			fdatasync(_btrees[i].db_fd);
		}
	}
}

static void bgsync_init()
{
	pthread_t t1;
	if((t1=pthread_create(&_bgsync,NULL,bgsync_func,NULL))!=0)
		abort();
	//pthread_join(t1,NULL);
}

/*Sequential read,and mark keys in BloomFilter
* This is very important to preheat datas.
*/
static int db_loadbloom(struct btree *btree)
{
	int sum=0;
	int i,super_size=sizeof(struct btree_super);
	uint64_t alloc=btree->alloc-super_size;
	int newsize=(sizeof(struct btree_table));
	lseek64(btree->fd,super_size, SEEK_SET);
	while(alloc>0){
		struct btree_table *table=malloc(newsize);
		read(btree->fd,table, newsize) ;
		if(table->size>0){
			sum+=table->size;
			for(i=0;i<table->size;i++){
				uint64_t offset=from_be64(table->items[i].offset);
				if(get_H(offset)==0)
					bloom_add(&_bloom,(const char*)table->items[i].sha1);
			}
		}
		free(table);
		alloc-=newsize;
	}
	return sum;
}

void db_init(int bufferpool_size,int isbgsync,uint64_t *sum)
{
	uint64_t s=0UL;
	int i;	
	llru_init(bufferpool_size);
	bloom_init(&_bloom,IDX_PRIME);

	for(i=0;i<DB_SLOT;i++){
		char pre[256]={0};
		sprintf(pre,"%s%d",DB_PREFIX,i);
		btree_init(&_btrees[i],pre,isbgsync);
		s+=db_loadbloom(&_btrees[i]);	
	}
	*sum=s;
	if(isbgsync)
		bgsync_init();
}


int db_add(const char *key,const char *value)
{
	uint64_t off;
	unsigned int slot=jdb_hash(key)%DB_SLOT;
	int isin=btree_get_index(&_btrees[slot],key);
	if(isin)
		return (0);

	off=btree_insert(&_btrees[slot],key,(const void*)value,strlen(value));
	if(off==0)
		return (0);
	bloom_add(&_bloom,key);
	return (1);
}


void *db_get(const char *key)
{
	void *v;
	int b;
	b=bloom_get(&_bloom,key);
	if(b!=0)
		return NULL;

	v=llru_get(key);
	if(v==NULL){
		char *k_tmp,*v_tmp;
		unsigned int slot=jdb_hash(key)%DB_SLOT;
		v=btree_get(&_btrees[slot],key);
		if(v==NULL)
			return NULL;

		k_tmp=strdup(key);
		v_tmp=strdup((char*)v);
		llru_set(k_tmp,v_tmp,strlen(k_tmp),strlen(v_tmp));
		return v;
	}else{
		return strdup((char*)v);
	}
}

void db_get_range(const char *begin,const char *end,struct nobj *obj,int *retcount)
{
	int i;	
	for(i=0;i<DB_SLOT;i++){
		btree_get_range(&_btrees[i], begin,end,obj,retcount);
	}
}


void db_remove(const char *key)
{
	unsigned int slot=jdb_hash(key)%DB_SLOT;
	btree_delete(&_btrees[slot],key);
	llru_remove(key);
}

void db_update(const char *key,const char *value)
{
	unsigned int slot=jdb_hash(key)%DB_SLOT;
	int isin=btree_get_index(&_btrees[slot],key);
	if(isin){
		db_remove(key);
		db_add(key,value);
	}else{
		db_add(key,value);
	}
}

void db_destroy()
{
	int i;
	for(i=0;i<DB_SLOT;i++)
		btree_close(&_btrees[i]);
	llru_free();
}
