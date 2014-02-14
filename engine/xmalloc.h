/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_XMALLOC_H_
#define nessDB_XMALLOC_H_

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>

void *xmalloc(size_t s);
void *xcalloc(size_t n, size_t s);
void *xmalloc_aligned(size_t alignment, size_t s);
void *xrealloc(void *p, size_t s);
void *xrealloc_aligned(void *p, size_t olds, size_t alignment, size_t s);
void *xmemdup(void *p, size_t s);
void *xmemmove(void *dst, void *src, size_t s);
void xfree(void *p);
void xcheck_all_free(void);
void xreset(void);

#endif /* nessDB_XMALLOC_H_ */
