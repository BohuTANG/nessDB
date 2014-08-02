/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * unrolled packed memory array
 */

#include "pma.h"

#define UNROLLED_LIMIT (48)
static inline void _array_extend(struct array *a)
{
	if ((a->used + 1) >= a->size) {
		a->size *= 2;
		a->elems = xrealloc(a->elems, a->size * sizeof(void*));
	}
}

static void _array_insertat(struct array *a, void *e, int i)
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

static inline int _array_findzero_upperbound(struct array *a, void *e, compare_func f)
{
	int low = 0;
	int high = a->used;

	while (low != high) {
		int mid = (low + high) / 2;

		if (f(e, a->elems[mid]) > 0)
			low = mid + 1;
		else
			high = mid;
	}

	return low;
}

struct array *_array_new(int reverse) {
	struct array *a = xcalloc(1, sizeof(*a));

	a->used = 0;
	a->size = reverse;
	a->elems = xcalloc(a->size, sizeof(void*));

	return a;
}

static void _array_free(struct array *a)
{
	xfree(a->elems);
	xfree(a);
}

static inline void _chain_extend(struct pma *p)
{
	if ((p->used + 1) >= p->size) {
		p->size *= 2;
		p->chain = xrealloc(p->chain, p->size * sizeof(void*));
	}
}

static void _chain_insertat(struct pma *p, struct array *a, int i)
{
	_chain_extend(p);
	if (i < p->used) {
		xmemmove(p->chain + i + 1,
		         p->chain + i,
		         (p->used - i) * sizeof(void*));
	}
	p->chain[i] = a;
	p->used++;
}

static inline int _chain_findzero_lowerbound(struct pma *p, void *e, compare_func f)
{
	int low  = 0;
	int high = p->used - 1;
	struct array *a;
	struct array **chain = p->chain;

	while (low <= high) {
		int mid = (low + high) / 2;
		a = chain[mid];
		if (f(*(a->elems), e) < 0)
			low  = mid + 1;
		else
			high = mid - 1;
	}

	if (low == p->used && low > 0)
		low--;

	return low;
}

static void _pma_insertat(struct pma *p, void *e, struct pma_coord *coord)
{
	struct array *array = NULL;
	int pma_used = p->used;
	int chain_idx = coord->chain_idx;
	int array_idx = coord->array_idx;

	if (pma_used == chain_idx) {
		if (pma_used == 0) {
			array = _array_new(UNROLLED_LIMIT);
			_chain_insertat(p, array, p->used);
		} else {
			array = p->chain[pma_used - 1];
		}
		_array_insertat(array, e, array->used);
	} else {
		array = p->chain[chain_idx];
		_array_insertat(array, e, array_idx);
	}

	if (array->used >= UNROLLED_LIMIT) {
		int half = array->used / 2;
		struct array *na = _array_new(UNROLLED_LIMIT);

		na->used = array->used - half;
		memcpy(na->elems, array->elems + half, na->used * sizeof(void*));
		array->used = half;
		_chain_insertat(p, na, chain_idx + 1);
	}
}

struct pma *pma_new(int reverse) {
	struct pma *p = xcalloc(1, sizeof(*p));

	p->used = 0;
	p->size = reverse;
	p->chain = xcalloc(p->size, sizeof(struct array*));

	return p;
}

void pma_insert(struct pma *p, void *e, compare_func f)
{
	int array_idx = 0;
	int chain_idx = _chain_findzero_lowerbound(p, e, f);

	if (chain_idx != p->used) {
		struct array *arr = p->chain[chain_idx];
		array_idx = _array_findzero_upperbound(arr, e, f);
	}

	struct pma_coord coord = {.chain_idx = chain_idx, .array_idx = array_idx};
	_pma_insertat(p, e, &coord);
	p->count++;
}

void pma_append(struct pma *p, void *e)
{
	int array_idx = 0;
	int chain_idx = 0;

	if (p->used > 0)
		chain_idx = p->used - 1;

	if (chain_idx > 0) {
		struct array *arr = p->chain[chain_idx];

		if (arr && (arr->used > 1))
			array_idx = arr->used - 1;
	}

	struct pma_coord coord = {.chain_idx = chain_idx, .array_idx = array_idx};
	_pma_insertat(p, e, &coord);

	p->count++;
}

uint32_t pma_count(struct pma *p)
{
	return p->count;
}

void pma_free(struct pma *p)
{
	int i;

	for (i = 0; i < p->used; i++)
		_array_free(p->chain[i]);

	xfree(p->chain);
	xfree(p);
}
