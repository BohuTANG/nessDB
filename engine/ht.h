#ifndef _HT_H
#define _HT_H

#include <stddef.h>

struct ht 
{
	size_t size;
	size_t cap;
	size_t (*hashfunc) (void *n);
	int (*cmpfunc) (void *a, void *b);
	struct ht_node **nodes;
};

struct ht_node 
{
	struct ht_node *next;
};


void ht_init(struct ht *ht, size_t cap);
void ht_set(struct ht *ht, struct ht_node *node);
struct ht_node* ht_get(struct ht *ht, void *k);
struct ht_node* ht_remove(struct ht *ht, void *k);
void ht_free(struct ht *ht);

#endif
