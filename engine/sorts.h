#ifndef _SORTS_H
#define _SORTS_H

#include "cola.h"

void cola_insertion_sort(struct cola_item *item, int len);
int cola_merge_sort(struct cola_item *c, struct cola_item *a, int alen, struct cola_item *b, int blen);
void dump_items(struct cola_item *a, int len);

#endif
