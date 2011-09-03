#ifndef _IDX_H
#define _IDX_H

#include <stdint.h>

struct idx 
{
	size_t size;
	size_t cap;
	size_t (*hashfunc)  (const char *k);
	size_t (*hashfunc1) (const char *k);
	struct idx_node **nodes;
};

struct idx_node 
{
	size_t 		hash;
	uint64_t 	value;
	struct idx_node *next;
};


void 		idx_init(struct idx *idx,size_t cap);
void 		idx_set(struct idx *idx,const char *k,uint64_t v);
uint64_t 	idx_get(struct idx *idx,const char *k);
void 		idx_remove(struct idx *idx,const char *k);
size_t	 	idx_size(struct idx *idx);
void 		idx_free(struct idx *idx);

#endif
