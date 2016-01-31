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
	char *ca = (char*)(a);
	char *cb = (char*)(b);

	return bt_compare_func_builtin(ca, 16, cb, 16);
}

void _pma_insert(void *extra)
{
	int i;
	struct random *rnd = rnd_new();

	struct pma_extra *pe = (struct pma_extra*)extra;
	struct pma *p = (struct pma*)pe->p;
	char *kbuf = xmalloc(17);
	for (i = pe->start; i < pe->end; i++) {

		int key = rnd_next(rnd);
		snprintf(kbuf, 16, "%016d", key);
		kbuf[16] = 0;
		pma_insert(p, kbuf, pma_compare_func, NULL);
	}
	xfree(pe);
}

int PMA_COUNT = 201510 * 2;
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
