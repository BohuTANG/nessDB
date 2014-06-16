/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_PMA_H_
#define nessDB_PMA_H_

#include "internal.h"

#define ARRAY_CACHE_SIZE (64)
typedef int (*compare_func)(void *, void *);

struct array {
	int size;
	int used;
	void **elems;
	uint64_t memory_used;
} __attribute__((aligned(ARRAY_CACHE_SIZE)));

struct pma {
	int size;
	int used;
	int count;
	struct array **chain;
	uint64_t memory_used;
};

struct pma *pma_new(int reverse);
void pma_insert(struct pma *p, void *e, int size, compare_func f);
void pma_resize(struct pma *p, int size);
int pma_count_calc(struct pma *p);
uint64_t pma_memsize_calc(struct pma *p);
void pma_free(struct pma *p);

#endif /* nessDB_PMA_H_ */
