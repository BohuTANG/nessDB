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

struct pma_coord {
	int chain_idx;
	int array_idx;
};

struct array {
	int size;
	int used;
	void **elems;
} __attribute__((aligned(ARRAY_CACHE_SIZE)));

struct pma {
	int size;
	int used;
	int count;
	struct array **chain;
};

struct pma *pma_new(int);
void pma_free(struct pma *);

void pma_insert(struct pma *, void *, compare_func f);
void pma_append(struct pma *, void *);
uint32_t pma_count(struct pma *);

#endif /* nessDB_PMA_H_ */
