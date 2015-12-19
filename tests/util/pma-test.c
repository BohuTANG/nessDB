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
	struct pma_coord pc;

	struct pma_extra *pe = (struct pma_extra*)extra;
	struct pma *p = (struct pma*)pe->p;

	for (i = pe->start; i < pe->end; i++) {
		int *val;
		int ret = pma_find_zero(p, &i, pma_compare_func, NULL, (void**)&val, &pc);
		if (ret != NESS_OK) {
			k = xmalloc(sizeof(int));
			*k = i;
			pma_insert(p, k, pma_compare_func, NULL);
		}
	}
}


int count = 2015102;
CTEST(pma, single_thread_insert)
{
	int i = 0;
	int thds = 1;
	struct pma_extra pe;
	struct pma *p = pma_new(1024);
	struct kibbutz *ktz = kibbutz_new(2);
	struct timespec ts1, ts2;

	pe.p = p;
	ngettime(&ts1);
	for (i = 0; i < thds; i++) {
		pe.start = i * (count) / thds;
		pe.end = (++i) * count / thds;
		kibbutz_enq(ktz, _pma_insert, &pe);
	}
	kibbutz_free(ktz);
	ngettime(&ts2);
	printf(" cost %lldms ", time_diff_ms(ts1, ts2));
	pma_free(p);
}

CTEST(pma, multi_thread_insert)
{
	int i = 0;
	int thds = 4;
	struct pma_extra pe;
	struct pma *p = pma_new(thds);
	struct kibbutz *ktz = kibbutz_new(8);
	struct timespec ts1, ts2;

	pe.p = p;
	ngettime(&ts1);
	for (i = 0; i < thds; i++) {
		pe.start = i * (count) / thds;
		pe.end = (++i) * count / thds;
		kibbutz_enq(ktz, _pma_insert, &pe);
	}
	kibbutz_free(ktz);
	ngettime(&ts2);
	printf(" cost %lldms ", time_diff_ms(ts1, ts2));
	pma_free(p);
}
