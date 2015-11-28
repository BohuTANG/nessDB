/*
 * Copyright (c) 2012-2015 The nessDB Authors. All rights reserved.
 * Code is licensed with BSD.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "pma.h"
#include "../ctest.h"

static int cmp(void *a, void *b)
{
	if (!a) return -1;
	if (!b) return +1;

	int ia = *(int*)a;
	int ib = *(int*)b;

	return ia - ib;
}

CTEST(pma, insert)
{
	struct pma  *p = pma_new(2);

	int i = 0;
	int c = 1000;
	int *arr = malloc(c * sizeof(int));

	for (i = 0; i < c; i++) {
		arr[i] = i;
		pma_insert(p, (void*)&arr[i], cmp);
	}

	// 1
	ASSERT_EQUAL(c, pma_count(p));

	// 2
	int *v;
	int k = 666;
	int r = pma_find_zero(p, (void*)&k, cmp, (void**) &v);
	ASSERT_EQUAL(1, r);
	ASSERT_TRUE(*(int*)v == k);

	k = 666;
	r = pma_find_minus(p, (void*)&k, cmp, (void**) &v);
	ASSERT_EQUAL(1, r);
	ASSERT_TRUE(*(int*)v == 665);

	k = 666;
	r = pma_find_plus(p, (void*)&k, cmp, (void**) &v);
	ASSERT_EQUAL(1, r);
	ASSERT_TRUE(*(int*)v == 667);

	free(arr);
	pma_free(p);
}
