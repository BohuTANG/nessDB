#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>
#include "bitwise.h"
#include "storage.h"
#include "lru.h"
#include "bloom.h"
#include "db.h"

#define BF_BIT_SIZE 	(1024*1024*8)
#define MAX_HITS 	(1024)

static struct bloom 	_bloom;
static struct lru	_lru;
static struct btree 	_btree;
static int 		_islru=0;



/*Sequential read,and mark keys in BloomFilter
* This is very important to preheat datas.
*/
static void db_load_bloom()
{
	bloom_init(&_bloom,BF_BIT_SIZE);
	int i,l,super_size=sizeof(struct btree_super),c=0;
	uint64_t alloc=_btree.alloc-super_size;
	int newsize=(sizeof(struct btree_table));
	lseek64(_btree.fd,super_size, SEEK_SET);
	while(alloc>0)
	{
		struct btree_table *table=malloc(newsize);
		int readr=read(_btree.fd,table, newsize) ;
		if(table->size>0)
		{
			c+=table->size;
			for(l=0;l<table->size;l++)
			{
				bloom_add(&_bloom,table->items[l].sha1);
			}
		}
		free(table);
		alloc-=newsize;
	}
}

void db_init(int lru_maxnum)
{
	if(lru_maxnum>0)
	 	_islru=1;
		
	lru_init(&_lru,lru_maxnum);

	btree_init(&_btree);
	db_load_bloom();	
}


int db_add(char* key,char* value)
{
	bloom_add(&_bloom,key);

	btree_insert(&_btree,(const uint8_t*)key,(const void*)value,strlen(value));
	return (1);
}


void *db_get(char* key)
{
	if(bloom_get(&_bloom,key)!=0)
		return NULL;

	size_t len;
	uint64_t val_offset;
	if(_islru)
	{
		uint64_t entry=lru_get(&_lru,key);
		if(entry>0)
			return btree_get_byoffset(&_btree,entry,&len);
		else
		{
			uint64_t val_offset;
			void *data=btree_get(&_btree,key,&len,&val_offset);
			lru_set(&_lru,key,val_offset);
			return data;
		}
	}
	else
		return btree_get(&_btree,key,&len,&val_offset);
}

void db_remove(char* key)
{
	btree_delete(&_btree,key);
	if(_islru)
		lru_remove(&_lru,key);
}

void db_destroy()
{
	if(_islru)
		lru_free(&_lru);
	bloom_free(&_bloom);
	btree_close(&_btree);
}
