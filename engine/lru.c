/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 * Double-Linked list & Hashtable & LRU writen by BohuTANG
 */

#include <stdlib.h>
#include <string.h>
#include "lru.h"
#include "debug.h"

#define PRIME (1301081)

/* ===========================  Double Linked List ============================== */

struct list *list_new()
{
	struct list *l = calloc(1, sizeof(struct list));

	if (!l) {
		__ERROR("list new error");
		return NULL;
	}

	return l;
}

void list_push(struct list *list, struct list_node *node)
{
	if (!node)
		return;

	if (!list->head) {
		list->head = node;
	} else {
		node->nxt = list->head;
		list->head->pre = node;
		list->head = node;
		if (list->tail == NULL)
				list->tail = list->head->nxt;
	}

	list->used_size += node->size;
	list->count++;
}

struct list_node * list_find(struct list *list, void *key)
{
	struct list_node *cur;

	if (list) {
		for (cur = list->head; cur; cur = cur->nxt) {
			int cmp = strcmp((char*)key, cur->sk.data);

			if (cmp == 0)
				return cur;
		}
	}

	return NULL;
}

void list_remove(struct list *list, struct list_node *node)
{
	if (!node)
		return;

	struct list_node *pre = node->pre;
	struct list_node *nxt = node->nxt;

	if (pre) {
		if (nxt) {
			pre->nxt = nxt;
			nxt->pre = pre;
		} else {
			pre->nxt = NULL;
			list->tail = pre;
		}
	} else {
		if (nxt) {
			nxt->pre = NULL;
			list->head = nxt;
		} else {
			list->head = NULL;
			list->tail = NULL;
		}
	}

	list->used_size -= node->size;
	list->count--;
}

void list_free_node(struct list *list, struct list_node *node)
{
	if (node) {
		list_remove(list, node);

		if (node->sk.data)
			free(node->sk.data);

		if (node->sv.data)
			free(node->sv.data);

		free(node);
	}
}

void list_reverse(struct list *list, void (*callback) (void *))
{
	struct list_node *node;

	for (node = list->head; node; node = node->nxt)
		callback((void*)node);
}

void list_free(struct list *list)
{
	if (list) {
		struct list_node *cur = list->head;
		struct list_node *nxt = cur;

		while (cur) {
			nxt = cur;
			if (cur->sk.data)
				free(cur->sk.data);
			if (cur->sv.data)
				free(cur->sv.data);

			nxt = cur->nxt;
			free(cur);
			cur = nxt;
		}

		free(list);
	}
}

/* ===========================  Hashtable  ============================== */

static inline uint64_t _djb_hash(const char *key)
{
	unsigned int h = 5381;

	while (*key) {
		h = ((h<< 5) + h) + (unsigned int) *key;  /* hash * 33 + c */
		++key;
	}

	return h;
}

static uint64_t _find_slot(struct hashtable *ht, void *key)
{
	return _djb_hash((const char*)key) % ht->cap;
}

struct hashtable *hashtable_new(uint64_t cap)
{
	struct hashtable *ht = calloc(1, sizeof(struct hashtable));

	if (!ht)
		return NULL;

	ht->cap = cap;
	ht->buckets = calloc(cap, sizeof(struct list*));

	return ht;
}

void hashtable_set(struct hashtable *ht, void *key, void *value)
{
	if (!key || !value)
		return;

	uint64_t slot = _find_slot(ht, key);

	if (ht->buckets[slot] == NULL)
		ht->buckets[slot] = list_new();

	list_push(ht->buckets[slot], (struct list_node*)value);
	ht->size++;
}

void *hashtable_get(struct hashtable *ht, void *key)
{
	if (!key)
		return NULL;

	uint64_t slot = _find_slot(ht, key);
	struct list *list = ht->buckets[slot];

	if (list) {
		return (void*) list_find(list, key);
	}

	return NULL;
}

void hashtable_remove(struct hashtable *ht, void *key)
{
	if (!key)
		return;

	uint64_t slot = _find_slot(ht, key);
	struct list *list = ht->buckets[slot];

	if (list) {
		struct list_node *node = list_find(list, key);

		if (node)
			list_remove(list, node);
	}
}

void hashtable_free(struct hashtable *ht)
{
	uint64_t i;
	for (i = 0; i < ht->cap; i++) {
		if (ht->buckets[i]) {
			free(ht->buckets[i]);
		}
	}

	free(ht->buckets);
	free(ht);
}

/* =========================== LRU ============================== */

struct lru *lru_new(uint64_t size)
{
	struct lru *lru = malloc(sizeof(struct lru));
	
	if (!lru) {
		__ERROR("lru new error");
		goto RET;
	}
	memset(lru, 0, sizeof(struct lru));

	lru->list = list_new();
	if (!lru->list) {
		__ERROR("list new error");
		goto RET;
	}

	lru->ht = hashtable_new(PRIME);;
	if (!lru->ht) {
		__ERROR("ht new error");
		goto RET; }

	lru->allow = size;
	return lru;

RET:
	if (lru) {
		if (lru->list)
			list_free(lru->list);

		if (lru->ht)
			hashtable_free(lru->ht);

		free(lru);
	}

	return NULL;
}

void _lru_check(struct lru *lru)
{
	if (lru->list->used_size > lru->allow) {
		struct list *list = lru->list;
		struct list_node *tail = list->tail;
		struct hashtable *ht = lru->ht;

		/* free list tail */
		if (tail) {
			hashtable_remove(ht, tail->sk.data);
			list_free_node(list, tail);
			lru->count--;
		}
	}
}

void lru_set(struct lru *lru, struct slice *sk, struct slice *sv)
{
	struct list_node *node;

	if (lru->allow == 0)
		return;

	_lru_check(lru);

	node = hashtable_get(lru->ht, sk->data);
	if (node) {
		hashtable_remove(lru->ht, sk->data);
		list_remove(lru->list, node);
		list_free_node(lru->list, node);
	}
	
	node = calloc(1, sizeof(struct list_node));
	char *kdata= malloc(sk->len + 1);
	memset(kdata, 0, sk->len + 1);
	memcpy(kdata, sk->data, sk->len);
	node->sk.len = sk->len;
	node->sk.data = kdata;

	char *vdata= malloc(sv->len + 1);
	memset(vdata, 0, sv->len + 1);
	memcpy(vdata, sv->data, sv->len);
	node->sv.len = sv->len;
	node->sv.data = vdata;
	node->size = (sk->len + sv->len);

	hashtable_set(lru->ht, sk->data, node);
	list_push(lru->list, node);
	lru->count++;
}

int lru_get(struct lru *lru, struct slice *sk, struct slice *ret)
{
	struct list_node *node;

	node = hashtable_get(lru->ht, sk->data);
	if (node) {
		ret->len = node->sv.len;
		char *data = malloc(ret->len + 1);
		memset(data, 0, ret->len + 1);
		memcpy(data, node->sv.data, ret->len);

		ret->data = data;

		return 1;
	}

	return 0;
}

void lru_remove(struct lru *lru, struct slice *sk)
{
	struct list_node *node;

	if (lru->allow == 0)
		return;

	node = hashtable_get(lru->ht, sk->data);
	if (node) {
		hashtable_remove(lru->ht, sk->data);
		list_free_node(lru->list, node);
	}
}

void lru_free(struct lru *lru)
{
	if (lru) {
		hashtable_free(lru->ht);
		list_free(lru->list);
		free(lru);
	}
}
