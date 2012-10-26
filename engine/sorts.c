/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include <stdlib.h>
#include <string.h>

#include "sorts.h"
#include "debug.h"

void cola_insertion_sort(struct cola_item *item, int len)
{
	int i, j;

	for (i = 0; i < len; i++) {
		struct cola_item v = item[i];

		for (j = i - 1; j >= 0; j--) {
			if (strcmp(item[j].data, v.data) <= 0) 
				break;

			memcpy(&item[j + 1], &item[j], ITEM_SIZE);
		}
		memcpy(&item[j + 1], &v, ITEM_SIZE);
	}
}

int cola_merge_sort(struct cola_item *c, struct cola_item *a, int alen, struct cola_item *b, int blen)
{
	int i, m = 0, n = 0, k;

	for (i = 0; (m < alen) && (n < blen);) {
		int cmp = strcmp(a[m].data, b[n].data);

		if (cmp == 0) {
			if (b[n].opt == 1) 
				memcpy(&c[i++], &b[n], ITEM_SIZE);

			n++;
			m++;
		} else if (cmp < 0) 
			memcpy(&c[i++], &a[m++], ITEM_SIZE);
		else 
			memcpy(&c[i++], &b[n++], ITEM_SIZE);
	}

	if (m == alen) {
		for (k = n; k < blen; k++)
			memcpy(&c[i++], &b[k], ITEM_SIZE);
	} else if (n == blen) { 
		for (k = m; k < alen; k++)
			memcpy(&c[i++], &a[k], ITEM_SIZE);
	}

	return i;
}

void dump_items(struct cola_item *item, int len)
{
	int i;

	__DEBUG("---------dumps:");
	for (i = 0; i < len; i++) {
		__DEBUG("\t--[%d]key:%s", i, item[i].data);
	}
	__DEBUG("----------------");
}
