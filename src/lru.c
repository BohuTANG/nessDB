#include <stdlib.h>
#include <string.h>
#include "uthash.h"
#include "lru.h"

#define MAX_CACHE_SIZE (1024*4*100)

typedef struct cache
{
	char* k;
	char* v;
	UT_hash_handle hh;
}cache_t;

static cache_t* _cache=NULL;


char* lru_find(char* k)
{
	cache_t* item;
	HASH_FIND_STR(_cache,k,item);
	if(item)
	{
		HASH_DELETE(hh,_cache,item);
		HASH_ADD_KEYPTR(hh,_cache,item->k,strlen(item->k),item);
		return item->v;
	}

	return NULL;
}

void lru_add(char* k,char* v)
{
	char* ktmp,*vtmp;
	cache_t* item,*tmp_item;
	item=(cache_t*)malloc(sizeof(cache_t));
	ktmp=malloc(strlen(k)+1);
	strcpy(ktmp,k);

	vtmp=malloc(strlen(v)+1);
	strcpy(vtmp,v);

	item->k=ktmp;
	item->v=vtmp;

	HASH_ADD_KEYPTR(hh,_cache,item->k,strlen(item->k),item);
	if(HASH_COUNT(_cache)>=MAX_CACHE_SIZE)
	{
		HASH_ITER(hh,_cache,item,tmp_item)
		{
			HASH_DELETE(hh,_cache,item);
			if(item->k)
				free(item->k);
			if(item->v)
				free(item->v);
			free(item);
			break;
		}
	}
}

