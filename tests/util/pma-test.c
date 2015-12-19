#include "u.h"
#include "ctest.h"

int pma_compare_func(void *a, void *b, void *e)
{
	(void)e;
	int ia = *(int*)(a);
	int ib = *(int*)(b);

	return (ia - ib);
}

CTEST(pma, insert)
{
	int i;
	int *k;

	struct pma *p = pma_new();

	for (i = 0; i < 1024; i++) {
		k = xmalloc(sizeof(int));
		*k = i;
		pma_insert(p, k, pma_compare_func, NULL);
	}

	pma_free(p);
}

