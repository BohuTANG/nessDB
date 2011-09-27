#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>
#include "bitwise.h"
#include "storage.h"
#include "idx.h"
#include "db.h"
#include "llru.h"

#define IDX_PRIME	(16785407)

static struct idx	_idx;
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
	void *v=llru_get((const char*)key);
	if(v==NULL){
		v=btree_get(&_btree,key);
		char *k_tmp=strdup(key);
		char *v_tmp=strdup((char*)v);
		int ret=llru_set(k_tmp,v_tmp,strlen(k_tmp),strlen(v_tmp));
		if(ret==0){
			free(k_tmp);
			free(v_tmp);
		}
	}

	return v;
}

void db_remove(char* key)
{
	btree_delete(&_btree,key);
	idx_remove(&_idx,key);
}

void db_destroy()
{
	idx_free(&_idx);
	btree_close(&_btree);
}
