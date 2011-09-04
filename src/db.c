#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>
#include "bitwise.h"
#include "storage.h"
#include "idx.h"
#include "db.h"

#define IDX_PRIME	(16785407)

static struct idx	_idx;
static struct btree 	_btree;



/*Sequential read,and mark keys in BloomFilter
* This is very important to preheat datas.
*/
static void db_load2mem()
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
				offset=from_be64(table->items[l].offset);
				if(get_H(offset)==0)
					idx_set(&_idx,table->items[l].sha1,offset);
			}
		}
		free(table);
		alloc-=newsize;
	}
}

void db_init()
{
	idx_init(&_idx,IDX_PRIME);

	btree_init(&_btree);
	db_load2mem();	
}


int db_add(char* key,char* value)
{
	
	uint64_t off=btree_insert(&_btree,(const uint8_t*)key,(const void*)value,strlen(value));
	if(off==0)
		return (0);

	uint64_t l_off=idx_get(&_idx,key);
	if(l_off==0)	
		idx_set(&_idx,key,off);
	else {
		idx_remove(&_idx,key);
		idx_set(&_idx,key,off);
	}
	return (1);
}


void *db_get(char* key)
{
	uint64_t val_offset=idx_get(&_idx,key);
	if(val_offset>0)
			return btree_get_byoffset(&_btree,val_offset);

	return btree_get(&_btree,key);
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
