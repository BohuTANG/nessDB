#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>

#include "storage.h"
#include "htable.h"
#include "bloom.h"
#include "db.h"

#define BF_SIZE 	(1024*1024*8)
#define MAX_HITS 	(1024)

static struct bloom 	_bloom;
static hashtable*	_ht;
struct btree 		_btree;
static int 		_lru=0;

static size_t round_power2(size_t val)
{
	size_t i = 1;
	while (i < val)
		i <<= 1;
	return i;
}


void db_init(int lru_maxnum)
{
	if(lru_maxnum>0)
	 	_lru=1;
		
	_ht=hashtable_create(lru_maxnum);
	bloom_init(&_bloom,BF_SIZE);

	btree_init(&_btree);
	db_walk();	
}

/*Sequential read,and mark keys in BloomFilter
* This is very important to preheat datas.
*/
void db_walk()
{
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
					bloom_add(&_bloom,table->items[l].sha1);
		}
		free(table);
		alloc-=newsize;
	}
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
	if(_lru)
	{
		uint64_t entry=hashtable_get(_ht,key);
		if(entry>0)
			return btree_get_byoffset(&_btree,entry,&len);
		else
		{
			uint64_t val_offset;
			void *data=btree_get(&_btree,key,&len,&val_offset);
			hashtable_set(_ht,key,val_offset);
			return data;
		}
	}
	else
		return btree_get(&_btree,key,&len,&val_offset);
}

void db_remove(char* key)
{
	btree_delete(&_btree,key);
	if(_lru)
		hashtable_remove(_ht,key);
}

void db_destroy()
{
	btree_close(&_btree);
}
