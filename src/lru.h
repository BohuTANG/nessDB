#ifndef _LRU_H
#define _LRU_H

#include <stdint.h>

struct lru 
{
	size_t size;
	size_t cap;
	size_t (*hashfunc)  (const char *k);
	size_t (*hashfunc1) (const char *k);
	struct lru_node **nodes;
};

struct lru_node 
{
	size_t 		hash;
	uint64_t 	value;
	struct lru_node *next;
};


void 		lru_init(struct lru *lru,size_t cap);
void 		lru_set(struct lru *lru,const char *k,uint64_t v);
uint64_t 	lru_get(struct lru *lru,const char *k);
void 		lru_remove(struct lru *lru,const char *k);
size_t	 	lru_size(struct lru *lru);
void 		lru_free(struct lru *lru);

#endif
