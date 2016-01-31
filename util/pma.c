/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 * unrolled packed memory array with Fine-Grained Lock
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
static inline int _array_find_greater_than(struct array *a,
        void *k,
        compare_func f,
        void *extra)
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
static inline int _array_find_less_than(struct array *a,
                                        void *k,
                                        compare_func f,
                                        void *extra)
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
static inline int _array_find_zero(struct array *a,
                                   void *k,
                                   compare_func f,
                                   void *extra,
                                   int *found)
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
		/* make sure no readers on this array */
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
	ness_mutex_init(&a->mtx);
	ness_rwlock_init(&a->rwlock, &a->mtx);
	ness_spinlock_init(&a->r_spinlock);
	ness_spinlock_init(&a->w_spinlock);

	return a;
}

static void _array_free(struct array *a)
{
	xfree(a->elems);
	xfree(a);
}

static inline void _slots_extend(struct pma *p)
{
	if ((p->used + 1) >= p->size) {
		p->size *= 2;
		p->slots = xrealloc(p->slots, p->size * sizeof(void*));
	}
}

static void _slots_insertat(struct pma *p, struct array *a, int i)
{
	_slots_extend(p);
	if (i < p->used) {
		xmemmove(p->slots + i + 1,
		         p->slots + i,
		         (p->used - i) * sizeof(void*));
	}
	p->slots[i] = a;
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
static inline int _slots_find_lowerbound(struct pma *p, void *k, compare_func f, void *extra)
{
	int lo = 0;

	int hi = p->used - 1;
	int best = hi;
	struct array **slots = p->slots;

	if (hi == 0)
		return 0;

	while (lo <= hi) {
		int cmp;
		int mid = (hi + lo) / 2;
		struct array *a;

		a = slots[mid];
		/* make sure no extending on this array */
		ness_spinlock_waitfree(&a->w_spinlock);
		ness_spinrlock(&a->r_spinlock);
		cmp = f(*(a->elems), k, extra);
		ness_spinrunlock(&a->r_spinlock);
		if (cmp <= 0) {
			best = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}

	return best;
}

void _array_unrolled(struct pma *p, int slot_idx)
{
	int half;
	struct array *old_array;
	struct array *new_array;

	ness_rwlock_write_lock(&p->slots_rwlock);
	old_array = p->slots[slot_idx];
	if (old_array->used < UNROLLED_LIMIT) {
		/* some one has unrolled before us */
		ness_rwlock_write_unlock(&p->slots_rwlock);
		return;
	}

	nassert(old_array->used >= UNROLLED_LIMIT);
	half = old_array->used / 2;

	new_array = _array_new(old_array->size);
	new_array->used = old_array->used - half;
	memcpy(new_array->elems, old_array->elems + half, new_array->used * sizeof(void*));
	old_array->used = half;
	_slots_insertat(p, new_array, slot_idx + 1);
	ness_rwlock_write_unlock(&p->slots_rwlock);
}

struct pma *pma_new(int reverse)
{
	struct pma *p = xcalloc(1, sizeof(*p));

	p->size = reverse;
	p->slots = xcalloc(p->size, sizeof(struct array*));
	ness_mutex_init(&p->mtx);
	ness_rwlock_init(&p->slots_rwlock, &p->mtx);

	p->slots[0] = _array_new(UNROLLED_LIMIT);
	p->used++;

	return p;
}

void pma_free(struct pma *p)
{
	int i;

	for (i = 0; i < p->used; i++)
		_array_free(p->slots[i]);

	ness_mutex_destroy(&p->mtx);
	xfree(p->slots);
	xfree(p);
}

void pma_insert(struct pma *p, void *k, compare_func f, void *extra)
{
	int array_idx = 0;
	int slot_idx = 0;
	int array_used = 0;
	struct array *arr;

	ness_rwlock_read_lock(&p->slots_rwlock);
	slot_idx  = _slots_find_lowerbound(p, k, f, extra);
	arr = p->slots[slot_idx];

	 ness_spinwlock(&arr->w_spinlock);
	 ness_spinlock_waitfree(&arr->r_spinlock);

	array_used = arr->used;
	array_idx = _array_find_greater_than(arr, k, f, extra);

	/* if array_idx is -1,
	   means that we got the end of the array
	 */
	if (nessunlikely(array_idx == -1))
		array_idx = arr->used;

	_array_insertat(arr, k, array_idx);
	atomic_fetch_and_inc(&p->count);
	 ness_spinwunlock(&arr->w_spinlock);
	ness_rwlock_read_unlock(&p->slots_rwlock);

	if ((array_used + 1) > UNROLLED_LIMIT)
		_array_unrolled(p, slot_idx);
}

void pma_append(struct pma *p, void *k, compare_func f, void *extra)
{
	int array_idx = 0;
	int slot_idx = 0;
	int array_used = 0;
	struct array *arr;
	(void)f;
	(void)extra;

	ness_rwlock_read_lock(&p->slots_rwlock);
	if (p->used > 0)
		slot_idx = p->used - 1;

	arr = p->slots[slot_idx];
	array_used = arr->used;

	if (array_used > 0)
		array_idx = array_used - 1;

	_array_insertat(arr, k, array_idx);
	atomic_fetch_and_inc(&p->count);
	ness_rwlock_read_unlock(&p->slots_rwlock);

	if (array_used > UNROLLED_LIMIT)
		_array_unrolled(p, slot_idx);

}

uint32_t pma_count(struct pma *p)
{
	uint32_t c = 0;
	ness_mutex_lock(&p->mtx);
	c = p->count;
	ness_mutex_unlock(&p->mtx);

	return c;
}

int pma_find_minus(struct pma *p,
                   void *k,
                   compare_func f,
                   void *extra,
                   void **retval,
                   struct pma_coord *coord)
{
	int slot_idx = 0;
	int array_idx = 0;
	int ret = NESS_NOTFOUND;
	struct array *arr;


	memset(coord, 0, sizeof(*coord));

try_again:
	ness_rwlock_read_lock(&p->slots_rwlock);
	slot_idx = _slots_find_lowerbound(p, k, f, extra);
	arr = p->slots[slot_idx];
	if (!ness_rwlock_try_read_lock(&arr->rwlock)) {
		ness_rwlock_read_unlock(&p->slots_rwlock);
		goto try_again;
	}

	array_idx = _array_find_less_than(arr, k, f, extra);
	if (nessunlikely(array_idx == -1)) {
		if (slot_idx > 0) {
			slot_idx -= 1;
			arr = p->slots[slot_idx];
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

	coord->slot_idx = slot_idx;
	coord->array_idx = array_idx == -1 ? 0 : array_idx;

	ness_rwlock_read_unlock(&arr->rwlock);
	ness_rwlock_read_unlock(&p->slots_rwlock);

	return ret;
}

int pma_find_plus(struct pma *p,
                  void *k,
                  compare_func f,
                  void *extra,
                  void **retval,
                  struct pma_coord *coord)
{
	int slot_idx = 0;
	int array_idx = 0;
	int ret = NESS_NOTFOUND;
	struct array *arr;

	memset(coord, 0, sizeof(*coord));

try_again:
	ness_rwlock_read_lock(&p->slots_rwlock);
	slot_idx = _slots_find_lowerbound(p, k, f, extra);
	arr = p->slots[slot_idx];
	if (!ness_rwlock_try_read_lock(&arr->rwlock)) {
		ness_rwlock_read_unlock(&p->slots_rwlock);
		goto try_again;
	}
	array_idx = _array_find_greater_than(arr, k, f, extra);

	if (nessunlikely(array_idx == -1)) {
		if (slot_idx < (p->used - 1)) {
			slot_idx += 1;
			arr = p->slots[slot_idx];
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

	coord->slot_idx = slot_idx;
	coord->array_idx = array_idx == -1 ? arr->used : array_idx;
	ness_rwlock_read_unlock(&arr->rwlock);
	ness_rwlock_read_unlock(&p->slots_rwlock);

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
	int slot_idx = 0;
	int array_idx = 0;
	int ret = NESS_NOTFOUND;
	struct array *arr;

	memset(coord, 0, sizeof(*coord));

try_again:
	ness_rwlock_read_lock(&p->slots_rwlock);
	slot_idx = _slots_find_lowerbound(p, k, f, extra);
	arr = p->slots[slot_idx];
	if (!ness_rwlock_try_read_lock(&arr->rwlock)) {
		ness_rwlock_read_unlock(&p->slots_rwlock);
		goto try_again;
	}
	array_idx = _array_find_zero(arr, k, f, extra, &found);

	/* got one value */
	if (found) {
		*retval = arr->elems[array_idx];
		ret = NESS_OK;
	} else {
		coord->slot_idx = slot_idx;
		coord->array_idx = array_idx;
	}
	ness_rwlock_read_unlock(&arr->rwlock);
	ness_rwlock_read_unlock(&p->slots_rwlock);

	return ret;
}
