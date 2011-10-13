#ifndef _HT_H
#define _HT_H

#include <stdint.h>

typedef enum {STRING=0,INT} KEYTYPE;

struct ht 
{
	size_t size;
	size_t cap;
	size_t (*hashfunc)  (const char *k);
	struct ht_node **nodes;
	KEYTYPE key_type;

};

struct ht_node 
{
	void* 		k;
	void*	 	v;
	struct ht_node *next;
};


void 		ht_init(struct ht *ht,size_t cap,KEYTYPE key_type);
void 		ht_set(struct ht *ht,void *k,void* v);
void*	 	ht_get(struct ht *ht,void *k);
void 		ht_remove(struct ht *ht,void *k);
void 		ht_free(struct ht *ht);

#endif
