/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _LRU_H
#define _LRU_H
#include "util.h"
 
struct hashtable {
	uint32_t size;
	uint64_t cap;
	struct hashtable_node **buckets;
};

struct hashtable_node {
	void *key;
	void *value;
	struct hashtable_node *nxt;
};

struct list{
	uint64_t count;
	uint64_t used;
	struct list_node *head;
	struct list_node *tail;
};

struct list_node {
	uint32_t size;
	struct slice sk;
	struct slice sv;
	struct list_node *nxt;
	struct list_node *pre;
};

struct lru {
	uint64_t allow;
	struct list *list;
	struct hashtable *ht;
};

/* =========================== Double Linked List ============================== */

struct list *list_new();
void list_push(struct list *list, struct list_node *node);
struct list_node *list_find(struct list *list, void *key);
void list_remove(struct list *list, struct list_node *node);
void list_free_node(struct list *list, struct list_node *node);
void list_reverse(struct list *list, void (*callback) (void *));
void list_free(struct list *list);

/* ===========================  Hashtable  ============================== */

struct hashtable *hashtable_new(uint64_t cap);
void hashtable_set(struct hashtable *ht, void *key, void *val);
void *hashtable_get(struct hashtable *ht, void *key);
void hashtable_remove(struct hashtable *ht, void *key);
void hashtable_free(struct hashtable *ht);

/* =========================== LRU ============================== */

struct lru *lru_new(uint64_t buffer_size);
void lru_set(struct lru *, struct slice *sk, struct slice *sv);
int lru_get(struct lru *lru, struct slice *sk, struct slice *ret);
void  lru_remove(struct lru *lru, struct slice *sk);
void lru_free(struct lru *lru);

#endif
