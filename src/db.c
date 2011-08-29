#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>

#include "btree.h"
#include "htable.h"
#include "db.h"

#define MAX_HITS (1024)

static hashtable*	_ht;
struct btree 		_btree;
static int 		_lru=0;


void db_init(int lru_maxnum)
{
	if(lru_maxnum>0)
	 	_lru=1;
		
	_ht=hashtable_create(lru_maxnum);
	btree_init(&_btree);
}

int db_add(char* key,char* value)
{
	btree_insert(&_btree,(const uint8_t*)key,(const void*)value,strlen(value));
	return (1);
}


void *db_get(char* key)
{
	size_t len;
	uint64_t val_offset;
	if(_lru)
	{
		uint64_t entry=hashtable_get(_ht,key);
		if(entry>0)
		{
			return btree_get_byoffset(&_btree,entry,&len);
		}
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
