/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CPAIR_H_
#define nessDB_CPAIR_H_

#include "internal.h"
#include "posix.h"
#include "node.h"

#define PAIR_LIST_SIZE (1<<20)

enum lock_type {
	L_READ,
	L_WRITE
};

struct cpair {
	NID k;
	struct node *v;
	struct cpair *list_next;
	struct cpair *list_prev;
	struct cpair *hash_next;

	ness_mutex_t pair_mtx;		/* barriers for pair */
	ness_rwlock_t disk_mtx;		/* barriers for reading/writing to disk */
	ness_rwlock_t value_lock;	/* barriers for value */
};

struct cpair *cpair_new(void);
void cpair_init(struct cpair *cp, struct node *value);

/*******************************
 * list
 ******************************/
struct cpair_list {
	struct cpair *head;
	struct cpair *last;
	ness_rwlock_t lock;	/* barriers for value */
};

struct cpair_htable {
	struct cpair **tables;
	ness_mutex_aligned_t *mutexes;	/* array lock */
};

struct cpair_list *cpair_list_new(void);
void cpair_list_free(struct cpair_list *list);
void cpair_list_add(struct cpair_list *list, struct cpair *pair);
void cpair_list_remove(struct cpair_list *list, struct cpair *pair);


/*******************************
 * hashtable
 ******************************/
struct cpair_htable *cpair_htable_new(void);
void cpair_htable_free(struct cpair_htable *table);
void cpair_htable_add(struct cpair_htable *table, struct cpair *pair);
void cpair_htable_remove(struct cpair_htable *table, struct cpair *pair);
struct cpair *cpair_htable_find(struct cpair_htable *table, NID key);

#endif /* nessDB_CPAIR_H_ */
