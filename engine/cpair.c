/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "cpair.h"

struct cpair *cpair_new()
{
	struct cpair *cp;

	cp = xcalloc(1, sizeof(*cp));
	mutex_init(&cp->pair_mtx);
	rwlock_init(&cp->disk_mtx);
	rwlock_init(&cp->value_lock);

	return cp;
}

void cpair_init(struct cpair *cp, struct node *value)
{
	cp->k = value->nid;
	cp->v = value;
}

/*******************************
 * list (in clock order)
 ******************************/
struct cpair_list *cpair_list_new()
{
	struct cpair_list *list;

	list = xcalloc(1, sizeof(*list));
	rwlock_init(&list->lock);

	return list;
}

void cpair_list_free(struct cpair_list *list)
{
	xfree(list);
}

void cpair_list_add(struct cpair_list *list, struct cpair *pair)
{
	if (list->head == NULL) {
		list->head = pair;
		list->last = pair;
	} else {
		pair->list_prev = list->last;
		list->last->list_next = pair;
		list->last = pair;
	}
}

void cpair_list_remove(struct cpair_list *list, struct cpair *pair)
{
	if (list->head == pair)
		list->head = pair->list_next;

	if (pair->list_next)
		pair->list_next->list_prev = pair->list_prev;

	if (pair->list_prev)
		pair->list_prev->list_next = pair->list_next;

}

/*******************************
 * hashtable
 ******************************/
struct cpair_htable *cpair_htable_new()
{
	int i;
	struct cpair_htable *table;

	table = xcalloc(1, sizeof(*table));
	table->tables = xcalloc(PAIR_LIST_SIZE, sizeof(*table->tables));
	table->mutexes = xcalloc(PAIR_LIST_SIZE, sizeof(*table->mutexes));
	for (i = 0; i < PAIR_LIST_SIZE; i++) {
		mutex_init(&table->mutexes[i].aligned_mtx);
	}

	return table;
}

void cpair_htable_free(struct cpair_htable *table)
{
	xfree(table->tables);
	xfree(table->mutexes);
	xfree(table);
}

void cpair_htable_add(struct cpair_htable *table, struct cpair *pair)
{
	int hash;

	hash = (pair->v->nid & (PAIR_LIST_SIZE - 1));
	pair->hash_next = table->tables[hash];
	table->tables[hash] = pair;
}

void cpair_htable_remove(struct cpair_htable *table, struct cpair *pair)
{
	int hash;

	hash = (pair->v->nid & (PAIR_LIST_SIZE - 1));
	if (table->tables[hash] == pair) {
		table->tables[hash] = pair->hash_next;
	} else {
		struct cpair *curr = table->tables[hash];
		while (curr->hash_next != pair) {
			curr = curr->hash_next;
		}
		curr->hash_next = pair->hash_next;
	}
}

struct cpair *cpair_htable_find(struct cpair_htable *table, NID key)
{
	int hash;
	struct cpair *curr;

	hash = (key & (PAIR_LIST_SIZE - 1));
	curr = table->tables[hash];
	while (curr) {
		if (curr->k == key) {
			return curr;
		}
		curr = curr->hash_next;
	}

	return NULL;
}
