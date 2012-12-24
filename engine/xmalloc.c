/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include "xmalloc.h"
#include "debug.h"

void _check_memory(void *p) 
{
	if (p == 0)
		__PANIC("memory allocation failed -- exiting.");

}

void *xmalloc(size_t n) 
{
	void *p = 0;

	p = malloc(n);
	_check_memory(p);
	return p;
}

void *xcalloc(size_t n, size_t s) 
{
	void *p = 0;

	p = calloc(n, s);
	_check_memory(p);
	return p;
}

void *xrealloc(void *p, size_t n) 
{
	p = realloc(p, n);
	_check_memory(p);
	return p;
}

void xfree(void *p)
{
	if (p)
		free(p);
}
