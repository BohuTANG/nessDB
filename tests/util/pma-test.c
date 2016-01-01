#include "u.h"
#include "ctest.h"

struct pma_extra {
	struct pma *p;
	int *ks;
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

	struct pma_extra *pe = (struct pma_extra*)extra;
	struct pma *p = (struct pma*)pe->p;
	int *ks = pe->ks;

	for (i = pe->start; i < pe->end; i++) {
		ks[i - pe->start]  = i;
		pma_insert(p, &ks[i - pe->start], pma_compare_func, NULL);
	}
	xfree(pe);
}

int PMA_COUNT = 201510 * 5;
int MAX_THREADS = 32;
void _pma_benchmark(int thds)
{
	int i = 0;
	int *ks;
	long long cost;
	struct pma_extra *pe;
	struct pma *p = pma_new(thds);
	struct kibbutz *ktz = kibbutz_new(thds + 1);
	struct timespec ts1, ts2;

	ks = xcalloc(PMA_COUNT, sizeof(*ks));

	ness_gettime(&ts1);
	for (i = 0; i < thds; i++) {
		pe = xcalloc(1, sizeof(*pe));
		pe->p = p;
		pe->ks = ks;
		pe->start = i * ((PMA_COUNT) / thds);
		pe->end = (i + 1) * ((PMA_COUNT) / thds);
		kibbutz_enq(ktz, _pma_insert, pe);
	}
	kibbutz_free(ktz);
	ness_gettime(&ts2);
	cost = ness_time_diff_ms(ts1, ts2);
	if (thds == 1)
		printf("\n");
	printf("\tthds=%d, cost=%lldms, %lld/sec\n", thds, cost, PMA_COUNT * 1000 / cost);
	pma_free(p);
	xfree(ks);
}

CTEST(pma, multi_threads_insert)
{
	int i;
	for (i = 1; i < (MAX_THREADS + 1); i++)
		_pma_benchmark(i);
}
