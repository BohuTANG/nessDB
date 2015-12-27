#include "u.h"
#include "ctest.h"

struct pma_extra {
	struct pma *p;
	int start;
	int end;
};

int pma_compare_func(void *a, void *b, void *e)
{
	(void)e;
	int ia = *(int*)(a);
	int ib = *(int*)(b);

	return (ia - ib);
}

void _pma_insert(void *extra)
{
	int i;
	int *k;

	struct pma_extra *pe = (struct pma_extra*)extra;
	struct pma *p = (struct pma*)pe->p;

	for (i = pe->start; i < pe->end; i++) {
		k = xmalloc(sizeof(int));
		*k = i;
		pma_insert(p, k, pma_compare_func, NULL);
	}
	xfree(pe);
}

#define PMA_COUNT (2015102)

void _pma_benchmark(int thds)
{
	int i = 0;
	struct pma_extra *pe;
	struct pma *p = pma_new(thds);
	struct kibbutz *ktz = kibbutz_new(thds + 1);
	struct timespec ts1, ts2;

	ngettime(&ts1);
	for (i = 0; i < thds; i++) {
		pe = xcalloc(1, sizeof(*pe));
		pe->p = p;
		pe->start = i * ((PMA_COUNT) / thds);
		pe->end = (i + 1) * ((PMA_COUNT) / thds);
		kibbutz_enq(ktz, _pma_insert, pe);
	}
	kibbutz_free(ktz);
	ngettime(&ts2);
	printf(" thds=%d, cost=%lldms ", thds, time_diff_ms(ts1, ts2));
	pma_free(p);
}

CTEST(pma, single_thread_insert)
{
	_pma_benchmark(1);
}

CTEST(pma, multi_thread_insert)
{
	_pma_benchmark(4);
}
