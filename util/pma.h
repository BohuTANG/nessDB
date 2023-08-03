/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_PMA_H_
#define nessDB_PMA_H_

typedef int (*compare_func)(void *, void *, void *);

struct pma_coord {
	int slot_idx;
	int array_idx;
};

struct array {
	int size;
	int used;
	void **elems;
	ness_mutex_t mtx;
	ness_rwlock_t rwlock;
	ness_spinlock r_spinlock; /* read spin lock */
	ness_spinlock w_spinlock; /* write spin lock */
} NESSPACKED;

struct pma {
	int size;
	int used;
	int count;
	struct array **slots;
	ness_mutex_t mtx;
	ness_rwlock_t slots_rwlock;
};

struct pma *pma_new(int);
void pma_free(struct pma *);

void pma_insert(struct pma *, void *, compare_func f, void *);
void pma_append(struct pma *, void *, compare_func f, void *);
uint32_t pma_count(struct pma *);

int pma_find_minus(struct pma *, void *, compare_func f, void *, void **, struct pma_coord *);
int pma_find_plus(struct pma *, void *, compare_func f, void *, void **, struct pma_coord *);
int pma_find_zero(struct pma *, void *, compare_func f, void *, void **, struct pma_coord *);

#endif /* nessDB_PMA_H_ */
