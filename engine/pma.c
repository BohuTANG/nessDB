/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * unrolled packed memory array
 */

#include "xmalloc.h"
#include "pma.h"

#define UNROLLED_LIMIT (128)
static inline void _array_extend(struct array *a)
{
	if ((a->used + 1) >= a->size) {
		a->size *= 2;
		a->elems = xrealloc(a->elems, a->size * sizeof(void*));
	}
}

void _array_insertat(struct array *a, void *e, int i)
{
	_array_extend(a);

	if (i < a->used) {
		xmemmove(a->elems + i + 1,
		         a->elems + i,
		         (a->used - i) * sizeof(void*));
	}
	a->elems[i] = e;
	a->used++;
}

void _array_pushback(struct array *a, void *e, int size)
{
	_array_insertat(a, e, a->used);
	a->memory_used += size;
}

static inline int _array_findzero(struct array *a, void *e, compare_func f)
{
	int first = 0;
	int last = a->used;

	while (first != last) {
		int mi = (first + last) / 2;

		if (f(e, a->elems[mi]) > 0)
			first = mi + 1;
		else
			last = mi;
	}

	return first;
}

void _array_insert(struct array *a, void *e, int size, compare_func f)
{
	int idx = _array_findzero(a, e, f);

	_array_insertat(a, e, idx);
	a->memory_used += size;
}

struct array *_array_new(int reverse) {
	struct array *a = xcalloc(1, sizeof(*a));

	a->used = 0;
	a->size = reverse;
	a->elems = xcalloc(a->size, sizeof(void*));

	return a;
}

void _array_free(struct array *a)
{
	xfree(a->elems);
	xfree(a);
}

static inline void _pma_extend(struct pma *p)
{
	if ((p->used + 1) >= p->size) {
		p->size *= 2;
		p->chain = xrealloc(p->chain, p->size * sizeof(void*));
	}
}

void _dump_chain(struct pma *p)
{
	int i;
	int k;
	printf("-----chain dump[size: %d, used: %d]:\n", p->size, p->used);

	for (i = 0; i < p->used; i++) {
		struct array *a = p->chain[i];
		printf("\t-----idx [%d], array used[%d], keys: ", i, a->used);
		for (k = 0; k < a->used; k++) {
			printf(" [%d] ", *(int*)a->elems[k]);
		}
		printf("\n");
	}
}

struct pma *pma_new(int reverse) {
	struct pma *p = xcalloc(1, sizeof(*p));

	p->used = 0;
	p->size = reverse;
	p->chain = xcalloc(p->size, sizeof(struct array*));

	return p;
}

void pma_resize(struct pma *p, int size)
{
	if (p->size < size) {
		p->size = size;
		p->chain = xcalloc(p->size, sizeof(struct array*));
	}
}

void pma_insertat(struct pma *p, struct array *a, int i)
{
	_pma_extend(p);
	if (i < p->used) {
		xmemmove(p->chain + i + 1,
		         p->chain + i,
		         (p->used - i) * sizeof(void*));
	}
	p->chain[i] = a;
	p->used++;
}

void pma_pushback(struct pma *p, struct array *a)
{
	pma_insertat(p, a, p->used);
}

int pma_count_calc(struct pma *p)
{
	int i;
	int all = 0;

	for (i = 0; i < p->used; i++)
		all += p->chain[i]->used;

	return all;
}

uint64_t pma_memsize_calc(struct pma *p)
{
	int i;
	uint64_t all = 0;

	for (i = 0; i < p->used; i++)
		all += p->chain[i]->memory_used;

	return all;
}

static inline int pma_findzero(struct pma *p, void *e, compare_func f)
{
	int first = 0;
	int last = p->used;

	while (first != last) {
		int mi = (first + last) / 2;
		struct array *a = p->chain[mi];

		if (f(e, a->elems[a->used - 1]) > 0)
			first = mi + 1;
		else
			last = mi;
	}

	return first;
}

void pma_insert(struct pma *p, void *e, int size, compare_func f)
{
	int chain_idx = 0;
	struct array *array = NULL;

	chain_idx = pma_findzero(p, e, f);
	if (p->used == chain_idx) {
		if (p->used == 0) {
			array = _array_new(UNROLLED_LIMIT);
			pma_pushback(p, array);
		} else {
			array = p->chain[p->used - 1];
		}
		_array_pushback(array, e, size);
		chain_idx = p->used - 1;
	} else {
		array = p->chain[chain_idx];
		_array_insert(array, e, size, f);
	}

	if (array->used >= UNROLLED_LIMIT) {
		int half = array->used / 2;

		/* deal with duplicate elements moving */
		if (half > 0) {
			while (f(array->elems[half - 1], array->elems[half]) == 0) {
				if (half == (array->used - 1))
					return;
				half++;
			}
		}

		struct array *na = _array_new(UNROLLED_LIMIT);

		na->used = array->used - half;
		memcpy(na->elems, array->elems + half, na->used * sizeof(void*));
		array->used = half;
		pma_insertat(p, na, chain_idx + 1);
	}
	p->count++;
	p->memory_used += size;
}

void pma_free(struct pma *p)
{
	int i;

	for (i = 0; i < p->used; i++)
		_array_free(p->chain[i]);

	xfree(p->chain);
	xfree(p);
}
