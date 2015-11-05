/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_PMA_H_
#define nessDB_PMA_H_

#define ARRAY_CACHE_SIZE (64)
typedef int (*compare_func)(void *, void *, void *);

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

void pma_insert(struct pma *, void *, compare_func f, void *);
void pma_insertat(struct pma *, void *, compare_func f, void *, struct pma_coord *);
void pma_append(struct pma *, void *, compare_func f, void *);
uint32_t pma_count(struct pma *);

int pma_find_minus(struct pma *, void *, compare_func f, void *, void **, struct pma_coord *);
int pma_find_plus(struct pma *, void *, compare_func f, void *, void **, struct pma_coord *);
int pma_find_zero(struct pma *, void *, compare_func f, void *, void **, struct pma_coord *);

#endif /* nessDB_PMA_H_ */
