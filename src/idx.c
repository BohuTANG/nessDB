/**
 * Compact-Hash-Tree implementation,used for idx
 */
#include <stdio.h>
#include <stdlib.h>
#include "hashes.h"
#include "idx.h"

static size_t find_slot(struct idx *idx,const char* key)
{
	return  idx->hashfunc(key) % idx->cap;
}

void	idx_init(struct idx *idx,size_t cap)
{
	idx->size = 0;
	idx->cap = cap;
	idx->nodes =calloc(cap, sizeof(struct idx_node*));
	idx->hashfunc=jdb_hash;
	idx->hashfunc1=sdbm_hash;
}

void	idx_set(struct idx *idx,const char* k,uint64_t v)
{
	size_t slot=find_slot(idx,k);
	struct idx_node *node=calloc(1, sizeof(struct idx_node));
	node->hash=idx->hashfunc1(k);
	node->next=idx->nodes[slot];
	node->value=v;

	idx->nodes[slot]=node;
	idx->size++;
}

uint64_t idx_get(struct idx *idx,const char* k)
{
	size_t slot=find_slot(idx,k);
	size_t hash=idx->hashfunc1(k);

	struct idx_node *node=idx->nodes[slot];
	while(node){
		if(hash==node->hash)
			return node->value;
		node=node->next;
	}
	return 0;
}

void	idx_remove(struct idx *idx,const char *k)
{
	size_t slot=find_slot(idx,k);
	size_t hash=idx->hashfunc1(k);

	struct idx_node *node=idx->nodes[slot],*prev=NULL;
	while(node){
		if(hash==node->hash){
			if(prev!=NULL)
				prev->next=node->next;
			else
				idx->nodes[slot]=node->next;
			idx->size--;

			if(node)
				free(node);
		}
		prev=node;
		node=node->next;	
	}
}

size_t	 idx_size(struct idx *idx)
{
	return idx->size;	
}


void 	idx_free(struct idx *idx)
{
	free(idx->nodes);
}

/*
int main()
{
	struct idx _idx;
	idx_init(&_idx,10000);
	idx_set(&_idx,"ab",100UL);
	idx_set(&_idx,"cd",5000000000UL);
	uint64_t  v=idx_get(&_idx,"cd");
	if(v!=0)
		printf("value is:%llu \n",v);
	
	idx_destroy(&_idx);	
}
*/

