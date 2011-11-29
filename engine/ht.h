#ifndef _HT_H
#define _HT_H

struct ht 
{
	size_t size;
	size_t cap;
	size_t (*hashfunc) (void  *k);
	int (*cmpfunc) (void *a, void *b);
	struct ht_node **nodes;
};

struct ht_node 
{
	void* 		k;
	void*	 	v;
	struct ht_node *next;
};


void ht_init(struct ht *ht, size_t cap);
void ht_set(struct ht *ht, void *k, void* v);
void* ht_get(struct ht *ht, void *k);
void ht_remove(struct ht *ht, void *k);
void ht_free(struct ht *ht);

#endif
