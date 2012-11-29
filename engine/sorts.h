#ifndef __nessDB_SORTS_H
#define __nessDB_SORTS_H

#include "db.h"
#include "compact.h"

void sst_insertion_sort(struct sst_item *item, int len);
int sst_merge_sort(struct compact *cpt, struct sst_item *c, struct sst_item *a_new, int alen, struct sst_item *b_old, int blen);

#endif
