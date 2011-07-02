#include <stdlib.h>
#include <string.h>
#include "hashtable.h"
#include "lru.h"

#define MAX_CACHE_SIZE (1024*4*100)

typedef struct cache
{
	char* k;
	char* v;
}cache_t;

static hashtable* _ht;
void lru_init()
{
		_ht=hashtable_create(MAX_CACHE_SIZE);
}

char* lru_find(char* k)
{
	cache_t* item=(cache_t*)hashtable_get(_ht,k);
	if(item)
	{
		hashtable_remove(_ht,k);
		hashtable_set(_ht,k,item);
		return item->v;
	}

	return NULL;
}

void lru_add(char* k,char* v)
{
	char* vtmp;
	cache_t* item,*tmp_item;
	item=(cache_t*)malloc(sizeof(cache_t));

	vtmp=malloc(strlen(v)+1);
	strcpy(vtmp,v);

	item->k=k;
	item->v=vtmp;

	hashtable_set(_ht,k,item);
	if(hashtable_count(_ht)>=MAX_CACHE_SIZE)
	{
		//todo delete
	}
}

