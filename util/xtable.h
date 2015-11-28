/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_XTABLE_H_
#define nessDB_XTABLE_H_

struct xtable_entry {
	void *v;
	struct xtable_entry *next;
};

struct xtable {
	uint32_t slot;
	int (*hash_func)(void *k);
	int (*compare_func)(void *a, void *b);
	struct xtable_entry **slots;
	ness_mutex_aligned_t *mutexes;	/* array lock */
};

struct xtable *xtable_new(uint32_t slot,
                          int (*hash_func)(void *k),
                          int (*compare_func)(void *a, void *b));

void xtable_free(struct xtable *xtbl);

void xtable_add(struct xtable *xtbl, void *v);
void xtable_remove(struct xtable *xtbl, void *v);
void *xtable_find(struct xtable *xtbl, void *k);


void xtable_slot_locked(struct xtable *xtbl, void *key);
void xtable_slot_unlocked(struct xtable *xtbl, void *key);

#endif /* nessDB_XTABLE_H_ */
