/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "atomic.h"
#include "xmalloc.h"

static uint64_t _n_mallocs = 0;

void _check_memory(void *p)
{
	assert(p != 0);
}

void *xmalloc(size_t s)
{
	void *p = 0;

	p = malloc(s);
	_check_memory(p);
	atomic64_increment(&_n_mallocs);

	return p;
}

void *xcalloc(size_t n, size_t s)
{
	void *p = 0;
	if (!n)
		return p;

	p = calloc(n, s);
	_check_memory(p);
	atomic64_increment(&_n_mallocs);

	return p;
}

void *xmalloc_aligned(size_t alignment, size_t s)
{
	int r;
	void *p = 0;

	r = posix_memalign(&p, alignment, s);
	(void)r;
	_check_memory(p);
	atomic64_increment(&_n_mallocs);

	return p;
}

void *xrealloc(void *p, size_t s)
{
	void *new_p;

	if (!p)
		return xcalloc(1, s);

	new_p = realloc(p, s);
	_check_memory(new_p);

	return new_p;
}

void *xrealloc_aligned(void *raw, size_t olds, size_t alignment, size_t s)
{
	int r;
	void *p = 0;

	r = posix_memalign(&p, alignment, s);
	(void)r;
	_check_memory(p);

	if (raw) {
		memcpy(p, raw, olds);
		xfree(raw);
	}
	atomic64_increment(&_n_mallocs);

	return p;
}

void *xmemdup(void *p, size_t s)
{
	void *new_p;

	new_p = calloc(1, s);
	_check_memory(new_p);
	atomic64_increment(&_n_mallocs);
	memcpy(new_p, p, s);

	return new_p;
}

void *xmemmove(void *dst, void *src, size_t s)
{
	void *new_p;

	new_p = memmove(dst, src, s);
	_check_memory(new_p);

	return new_p;
}

void xfree(void *p)
{
	if (p) {
		assert(_n_mallocs > 0);
		atomic64_decrement(&_n_mallocs);
		free(p);
	}
}

void xreset(void)
{
	_n_mallocs = 0;
}

void xcheck_all_free(void)
{
	if (_n_mallocs > 0)
		printf("--oops, n_mallocs is [%" PRIu64 "]\n", _n_mallocs);

	assert(_n_mallocs == 0);
}
