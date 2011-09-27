#include <stdio.h>
#include <stdlib.h>
#include "hashes.h"
#include "ht.h"

static size_t find_slot(struct ht *ht,const char* key)
{
	return  ht->hashfunc(key) % ht->cap;
}

void	ht_init(struct ht *ht,size_t cap)
{
	ht->size = 0;
	ht->cap = cap;
	ht->nodes =calloc(cap, sizeof(struct ht_node*));
	ht->hashfunc=jdb_hash;
}

void	ht_set(struct ht *ht,const char* k,void* v)
{
	size_t slot=find_slot(ht,k);
	struct ht_node *node=calloc(1, sizeof(struct ht_node));
	node->next=ht->nodes[slot];
	node->k=(char*)k;
	node->v=v;

	ht->nodes[slot]=node;
	ht->size++;
}

void* ht_get(struct ht *ht,const char* k)
{
	size_t slot=find_slot(ht,k);

	struct ht_node *node=ht->nodes[slot];
	while(node){
		if(strcmp(k,node->k)==0)
			return node->v;
		node=node->next;
	}
	return NULL;
}

void	ht_remove(struct ht *ht,const char *k)
{
	size_t slot=find_slot(ht,k);

	struct ht_node *node=ht->nodes[slot],*prev=NULL;
	while(node){
		if(strcmp(k,node->k)==0){
			if(prev!=NULL)
				prev->next=node->next;
			else
				ht->nodes[slot]=node->next;
			ht->size--;

			if(node)
				free(node);
		}
		prev=node;
		node=node->next;	
	}
}



void 	ht_free(struct ht *ht)
{
	free(ht->nodes);
}

