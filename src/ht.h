#ifndef _HT_H
#define _HT_H

#include <stdint.h>

struct ht 
{
	size_t size;
	size_t cap;
	size_t (*hashfunc)  (const char *k);
	struct ht_node **nodes;
};

struct ht_node 
{
	char* 		k;
	void*	 	v;
	struct ht_node *next;
};


void 		ht_init(struct ht *ht,size_t cap);
void 		ht_set(struct ht *ht,const char *k,void* v);
void*	 	ht_get(struct ht *ht,const char *k);
void 		ht_remove(struct ht *ht,const char *k);
void 		ht_free(struct ht *ht);

#endif
