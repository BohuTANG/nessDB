/**
 * Compact-Hash-Tree implementation,used for LRU
 */
#include <stdio.h>
#include <stdlib.h>
#include "hashes.h"
#include "lru.h"

static size_t find_slot(struct lru *lru,const char* key)
{
	return  lru->hashfunc(key) % lru->cap;
}

void	lru_init(struct lru *lru,size_t cap)
{
	lru->size = 0;
	lru->cap = cap;
	lru->nodes =calloc(cap, sizeof(struct lru_node*));
	lru->hashfunc=jdb_hash;
	lru->hashfunc1=sdbm_hash;
}

void	lru_set(struct lru *lru,const char* k,uint64_t v)
{
	size_t slot=find_slot(lru,k);
	struct lru_node *node=calloc(1, sizeof(struct lru_node));
	node->hash=lru->hashfunc1(k);
	node->next=lru->nodes[slot];
	node->value=v;

	lru->nodes[slot]=node;
	lru->size++;
}

uint64_t lru_get(struct lru *lru,const char* k)
{
	size_t slot=find_slot(lru,k);
	size_t hash=lru->hashfunc1(k);

	struct lru_node *node=lru->nodes[slot];
	while(node){
		if(hash==node->hash)
			return node->value;
		node=node->next;
	}
	return 0;
}

void	lru_remove(struct lru *lru,const char *k)
{
	size_t slot=find_slot(lru,k);
	size_t hash=lru->hashfunc1(k);

	struct lru_node *node=lru->nodes[slot],*prev=NULL;
	while(node){
		if(hash==node->hash){
			if(prev!=NULL)
				prev->next=node->next;
			else
				lru->nodes[slot]=node->next;
			lru->size--;

			if(node)
				free(node);
		}
		prev=node;
		node=node->next;	
	}
}

size_t	 lru_size(struct lru *lru)
{
	return lru->size;	
}


void 	lru_free(struct lru *lru)
{
	free(lru->nodes);
}

/*
int main()
{
	struct lru _lru;
	lru_init(&_lru,10000);
	lru_set(&_lru,"ab",100UL);
	lru_set(&_lru,"cd",5000000000UL);
	uint64_t  v=lru_get(&_lru,"cd");
	if(v!=0)
		printf("value is:%llu \n",v);
	
	lru_destroy(&_lru);	
}
*/

