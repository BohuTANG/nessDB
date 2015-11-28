/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"

struct xtable *xtable_new(uint32_t slot,
                          int (*hash_func)(void *k),
                          int (*compare_func)(void *a, void *b))
{
	uint32_t i;
	struct xtable *xtbl;

	xtbl = xcalloc(1, sizeof(*xtbl));
	xtbl->slot = slot;
	xtbl->hash_func = hash_func;
	xtbl->compare_func = compare_func;
	xtbl->slots = xcalloc(slot, sizeof(*xtbl->slots));
	xtbl->mutexes = xcalloc(slot, sizeof(*xtbl->mutexes));

	for (i = 0; i < slot; i++) {
		mutex_init(&xtbl->mutexes[i].aligned_mtx);
	}

	return xtbl;
}

void xtable_free(struct xtable *xtbl)
{
	uint32_t i;
	struct xtable_entry *curr;
	struct xtable_entry *next;

	for (i = 0; i < xtbl->slot; i++) {
		curr = xtbl->slots[i];
		while (curr) {
			next = curr->next;
			xfree(curr);
			curr = next;
		}
		mutex_destroy(&xtbl->mutexes[i].aligned_mtx);
	}

	xfree(xtbl->slots);
	xfree(xtbl->mutexes);
	xfree(xtbl);
}

void xtable_add(struct xtable *xtbl, void *v)
{
	int hash;
	struct xtable_entry *curr;

	hash = xtbl->hash_func(v) & (xtbl->slot - 1);
	curr = xcalloc(1, sizeof(*curr));
	curr->v = v;
	curr->next = xtbl->slots[hash];
	xtbl->slots[hash] = curr;
}

void xtable_remove(struct xtable *xtbl, void *v)
{
	int hash;
	struct xtable_entry *curr;
	struct xtable_entry *prev = NULL;

	hash = xtbl->hash_func(v) & (xtbl->slot - 1);
	curr = xtbl->slots[hash];
	while (curr && curr->v != v) {
		prev = curr;
		curr = curr->next;
	}

	if (curr) {
		if (prev)
			prev->next = curr->next;
		else
			xtbl->slots[hash] = curr->next;
		xfree(curr);
	}
}

void *xtable_find(struct xtable *xtbl, void *k)
{
	int hash;
	struct xtable_entry *curr;

	hash = xtbl->hash_func(k) & (xtbl->slot - 1);
	curr = xtbl->slots[hash];
	while (curr) {
		if (xtbl->compare_func(curr->v, k) == 0)
			return curr->v;
		curr = curr->next;
	}

	return NULL;
}

void xtable_slot_locked(struct xtable *xtbl, void *v)
{
	int hash;

	hash = xtbl->hash_func(v) & (xtbl->slot - 1);
	mutex_lock(&xtbl->mutexes[hash].aligned_mtx);
}

void xtable_slot_unlocked(struct xtable *xtbl, void *v)
{
	int hash;

	hash = xtbl->hash_func(v) & (xtbl->slot - 1);
	mutex_unlock(&xtbl->mutexes[hash].aligned_mtx);
}
