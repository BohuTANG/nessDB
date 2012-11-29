/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "sorts.h"

#define COLA_ITEM_SIZE (sizeof(struct sst_item))
void sst_insertion_sort(struct sst_item *item, int len)
{
	int i, j;

	for (i = 0; i < len; i++) {
		int cmp;
		struct sst_item v = item[i];

		for (j = i - 1; j >= 0; j--) {
			cmp = strcmp(item[j].data, v.data);
			if (cmp <= 0) {

				/* covert the old version */
				if (cmp == 0)
					memcpy(&item[j], &v, COLA_ITEM_SIZE); 

				break;
			}

			memcpy(&item[j + 1], &item[j], COLA_ITEM_SIZE);
		}
		memcpy(&item[j + 1], &v, COLA_ITEM_SIZE);
	}
}

int sst_merge_sort(struct compact *cpt, struct sst_item *c,
				   struct sst_item *a_new, int alen,
				   struct sst_item *b_old, int blen)
{
	int i, m = 0, n = 0, k;

	for (i = 0; (m < alen) && (n < blen);) {
		int cmp;

		cmp = strcmp(a_new[m].data, b_old[n].data);
		if (cmp == 0) {
			memcpy(&c[i++], &a_new[m], COLA_ITEM_SIZE);

			/* add delete slot to cpt */
			if (a_new[m].opt == 0 && a_new[m].vlen > 0) 
				if (cpt)
					cpt_add(cpt, a_new[m].vlen, a_new[m].offset); 
			n++;
			m++;
		} else if (cmp < 0) 
			memcpy(&c[i++], &a_new[m++], COLA_ITEM_SIZE);
		else 
			memcpy(&c[i++], &b_old[n++], COLA_ITEM_SIZE);
	}

	if (m == alen) {
		for (k = n; k < blen; k++)
			memcpy(&c[i++], &b_old[k], COLA_ITEM_SIZE);
	} else if (n == blen) { 
		for (k = m; k < alen; k++)
			memcpy(&c[i++], &a_new[k], COLA_ITEM_SIZE);
	}

	return i;
}
