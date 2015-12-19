#include "u.h"
#include "ctest.h"

int test_hash_func(void *k)
{
	return *(int*)(k);
}

int test_compare_func(void *a, void *b)
{
	int ia = *(int*)(a);
	int ib = *(int*)(b);

	return (ia - ib);
}

CTEST(xtable, insert)
{
	int i;
	int *k;
	int *ret;

	struct xtable *xtbl = xtable_new(32,
	                                 test_hash_func,
	                                 test_compare_func);

	for (i = 0; i < 1024; i++) {
		k = xmalloc(sizeof(int));
		*k = i;
		xtable_add(xtbl, k);
	}

	for (i = 1023; i >= 0 ; i--) {
		ret = xtable_find(xtbl, &i);
		ASSERT_EQUAL(*ret, i);
		xtable_remove(xtbl, ret);
		xfree(ret);

		ret = xtable_find(xtbl, &i);
		ASSERT_NULL(ret);
	}

	xtable_free(xtbl);
}

