/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 * unrolled packed memory array
 */

#include "u.h"

#define UNROLLED_LIMIT (256)

/*
 * EFFECT:
 *	- binary search with upper-bound
 *	- find the 1st idx which array[idx] > e
 * RETURN:
 *	- return 0 when e < array[0]
 *	- return -1 when e > array[max]
 */
static inline int _array_find_greater_than(struct array *a, void *k, compare_func f, void *extra)
{
	int lo = 0;
	int hi = a->used - 1;
	int best = -1;

	while (lo <= hi) {
		/* we assume that mid never overflow */
		int mid = (lo + hi) / 2;
		int cmp = f(a->elems[mid], k, extra);

		if (cmp > 0) {
			best = mid;
			hi = mid - 1;
		} else {
			lo = mid + 1;
		}
	}

	return best;
}

/*
 * EFFECT:
 *	- binary search with upper-bound
 *	- find the 1st idx which array[idx]
 * RETURN:
 *	- return -1 when e < array[0]
 *	- return max idx when array[max] < e
 */
static inline int _array_find_less_than(struct array *a, void *k, compare_func f, void *extra)
{
	int lo = 0;
	int hi = a->used - 1;
	int best = -1;

	nassert(a);
	while (lo <= hi) {
		int mid = (hi + lo) / 2;
		int cmp = f(a->elems[mid], k, extra);

		if (cmp < 0) {
			best = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}

	return best;
}

/*
 * EFFECT:
 *	- binary search
 *	- find the 1st idx which e == array[idx]
 */
static inline int _array_find_zero(struct array *a, void *k, compare_func f, void *extra, int *found)
{
	int lo = 0;
	int hi = a->used - 1;
	int best = hi;

	nassert(a);
	while (lo <= hi) {
		int mid = (hi + lo) / 2;
		int cmp = f(a->elems[mid], k, extra);

		if (cmp > 0) {
			best = mid;
			hi = mid - 1;
		} else if (cmp == 0) {
			*found = 1;
			break;
		} else {
			lo = mid + 1;
		}
	}

	return best;
}

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

struct array *_array_new(int reverse)
{
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

/*
 * EFFECT:
 *	- binary search with lower-bound
 *	- find the 1st idx which array[idx] <= e
 * RETURN:
 *	- return 0 when e < array[0]
 *	- return max  when e > array[max]
 */
static inline int _chain_find_lowerbound(struct pma *p, void *k, compare_func f, void *extra)
{
	int lo = 0;

	int hi = p->used - 1;
	int best = hi;
	struct array **chain = p->chain;

	if (hi == 0)
		return 0;

	while (lo <= hi) {
		int cmp;
		int mid = (hi + lo) / 2;
		struct array *a;

		a = chain[mid];
		cmp = f(*(a->elems), k, extra);
		if (cmp <= 0) {
			best = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}

	return best;
}

void _array_unrolled(struct pma *p, int chain_idx)
{
	int half;
	struct array *array;
	struct array *new_array;


	if (!rwlock_try_write_lock(&p->chain_rwlock)) {
		return;
	}

	array = p->chain[chain_idx];
	nassert(array->used >= UNROLLED_LIMIT);
	half = array->used / 2;

	new_array = _array_new(array->size);
	new_array->used = array->used - half;
	memcpy(new_array->elems, array->elems + half, new_array->used * sizeof(void*));
	array->used = half;
	_chain_insertat(p, new_array, chain_idx + 1);
	rwlock_write_unlock(&p->chain_rwlock);
}

struct pma *pma_new(int reverse)
{
	struct pma *p = xcalloc(1, sizeof(*p));

	p->size = reverse;
	p->chain = xcalloc(p->size, sizeof(struct array*));
	mutex_init(&p->mtx);
	ness_rwlock_init(&p->chain_rwlock, &p->mtx);

	p->chain[0] = _array_new(UNROLLED_LIMIT);
	p->used++;

	return p;
}

void pma_free(struct pma *p)
{
	int i;

	for (i = 0; i < p->used; i++)
		_array_free(p->chain[i]);

	mutex_destroy(&p->mtx);
	xfree(p->chain);
	xfree(p);
}

void pma_insert(struct pma *p, void *k, compare_func f, void *extra)
{
	int array_idx = 0;
	int chain_idx = 0;
	int array_used = 0;
	struct array *arr;

try_again:
	rwlock_read_lock(&p->chain_rwlock);
	chain_idx  = _chain_find_lowerbound(p, k, f, extra);
	arr = p->chain[chain_idx];
	if (!atomic_compare_and_swap(&arr->latch, 0, 1)) {
		rwlock_read_unlock(&p->chain_rwlock);
		goto try_again;
	}
	array_used = arr->used;
	array_idx = _array_find_greater_than(arr, k, f, extra);

	/* if array_idx is -1, means that we got the end of the array */
	if (nessunlikely(array_idx == -1))
		array_idx = arr->used;

	_array_insertat(arr, k, array_idx);
	atomic_fetch_and_inc(&p->count);
	if (!atomic_compare_and_swap(&arr->latch, 1, 0))
		abort();
	rwlock_read_unlock(&p->chain_rwlock);

	if ((array_used+1) >= UNROLLED_LIMIT)
		_array_unrolled(p, chain_idx);

}

void pma_append(struct pma *p, void *k, compare_func f, void *extra)
{
	int array_idx = 0;
	int chain_idx = 0;
	int array_used = 0;
	struct array *arr;
	(void)f;
	(void)extra;

	rwlock_write_lock(&p->chain_rwlock);

	if (p->used > 0)
		chain_idx = p->used - 1;

	arr = p->chain[chain_idx];
	array_used = arr->used;

	if (array_used > 0)
		array_idx = array_used - 1;

	_array_insertat(arr, k, array_idx);
	p->count++;
	rwlock_write_unlock(&p->chain_rwlock);

	if (array_used > UNROLLED_LIMIT)
		_array_unrolled(p, chain_idx);

}

uint32_t pma_count(struct pma *p)
{
	uint32_t c = 0;
	mutex_lock(&p->mtx);
	c = p->count;
	mutex_unlock(&p->mtx);

	return c;
}

int pma_find_minus(struct pma *p,
                   void *k,
                   compare_func f,
                   void *extra,
                   void **retval,
                   struct pma_coord *coord)
{
	int chain_idx = 0;
	int array_idx = 0;
	int ret = NESS_NOTFOUND;
	struct array *arr;

	if (p->used == 0)
		return ret;

	rwlock_read_lock(&p->chain_rwlock);
	chain_idx = _chain_find_lowerbound(p, k, f, extra);
	arr = p->chain[chain_idx];
	if (arr)
		array_idx = _array_find_less_than(arr, k, f, extra);
	if (nessunlikely(array_idx == -1)) {
		if (chain_idx > 0) {
			chain_idx -= 1;
			arr = p->chain[chain_idx];
			if (arr->used > 0) {
				*retval = arr->elems[arr->used - 1];
				array_idx = arr->used - 1;
				ret = NESS_OK;
			}
		}
	} else {
		*retval = arr->elems[array_idx];
		ret = NESS_OK;
	}

	coord->chain_idx = chain_idx;
	coord->array_idx = array_idx == -1 ? 0 : array_idx;

	rwlock_read_unlock(&p->chain_rwlock);

	return ret;
}

int pma_find_plus(struct pma *p,
                  void *k,
                  compare_func f,
                  void *extra,
                  void **retval,
                  struct pma_coord *coord)
{
	int chain_idx = 0;
	int array_idx = 0;
	int ret = NESS_NOTFOUND;
	struct array *arr;

	rwlock_read_lock(&p->chain_rwlock);
	chain_idx = _chain_find_lowerbound(p, k, f, extra);
	arr = p->chain[chain_idx];
	array_idx = _array_find_greater_than(arr, k, f, extra);

	if (nessunlikely(array_idx == -1)) {
		if (chain_idx < (p->used - 1)) {
			chain_idx += 1;
			arr = p->chain[chain_idx];
			if (arr->used > 0) {
				*retval = arr->elems[0];
				array_idx = 0;
				ret = NESS_OK;
			}
		}
	} else {
		*retval = arr->elems[array_idx];
		ret = NESS_OK;
	}

	coord->chain_idx = chain_idx;
	coord->array_idx = array_idx == -1 ? arr->used : array_idx;
	rwlock_read_unlock(&p->chain_rwlock);

	return ret;
}

int pma_find_zero(struct pma *p,
                  void *k,
                  compare_func f,
                  void *extra,
                  void **retval,
                  struct pma_coord *coord)
{
	int found = 0;
	int chain_idx = 0;
	int array_idx = 0;
	int ret = NESS_NOTFOUND;
	struct array *arr;

	memset(coord, 0, sizeof(*coord));
	rwlock_read_lock(&p->chain_rwlock);
	chain_idx = _chain_find_lowerbound(p, k, f, extra);
	arr = p->chain[chain_idx];
	array_idx = _array_find_zero(arr, k, f, extra, &found);

	/* got one value */
	if (found) {
		*retval = arr->elems[array_idx];
		ret = NESS_OK;
	} else {
		coord->chain_idx = chain_idx;
		coord->array_idx = array_idx;
	}
	rwlock_read_unlock(&p->chain_rwlock);

	return ret;
}
