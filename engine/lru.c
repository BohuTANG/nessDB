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
#include "xmalloc.h"

#define PRIME (1301081)

/* ===========================  Double Linked List ============================== */

struct list *list_new()
{
	struct list *l = xcalloc(1, sizeof(struct list));

	return l;
}

void list_push(struct list *list, struct list_node *node)
{
	if (!node)
		return;

	if (list->head != NULL) {
		node->nxt = list->head;
		list->head->pre = node;
		list->head = node;
	} else {
		list->head = node;
		list->tail = node;
	}

	list->used += node->size;
	list->count++;
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
			list->tail = pre;
			pre->nxt = NULL;
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

	list->used -= node->size;
	list->count--;

	if (node->sk.data)
		free(node->sk.data);
	if (node->sv.data)
		free(node->sv.data);
	free(node);
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
			nxt = cur->nxt;
			list_remove(list, cur);
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
	struct hashtable *ht = xcalloc(1, sizeof(struct hashtable));

	ht->cap = cap;
	ht->buckets = xcalloc(cap, sizeof(struct hashtable_node*));

	return ht;
}

void hashtable_set(struct hashtable *ht, void *key, void *value)
{
	if (!key || !value)
		return;

	struct hashtable_node *node;
	uint64_t slot = _find_slot(ht, key);

	node = xcalloc(1, sizeof(struct hashtable_node));
	node->key = key;
	node->value = value;

	node->nxt = ht->buckets[slot];
	ht->buckets[slot] = node;


	ht->size++;
}

void *hashtable_get(struct hashtable *ht, void *key)
{
	if (!key)
		return NULL;

	uint64_t slot = _find_slot(ht, key);
	struct hashtable_node *node;

	node = ht->buckets[slot];
	while (node) {
		if (strcmp((char*)key, (char*)node->key) == 0)
			return node->value;

		node = node->nxt;
	}
	
	return NULL;
}

void hashtable_remove(struct hashtable *ht, void *key)
{
	if (!key)
		return;

	uint64_t slot = _find_slot(ht, key);
	struct hashtable_node *cur, *prev = NULL;

	cur = ht->buckets[slot];
	while (cur) {
		if (strcmp((char*)key, (char*)cur->key) == 0) {
			if (prev != NULL)
				prev->nxt = cur->nxt;
			else
				ht->buckets[slot] = cur->nxt;
			if (cur)
				free(cur);
			ht->size--;
			return;
		}

		prev = cur;
		cur = cur->nxt;	
	}
}

void hashtable_free(struct hashtable *ht)
{
	uint64_t i;

	for (i = 0; i < ht->cap; i++) {
		if (ht->buckets[i]) {
			struct hashtable_node *cur = ht->buckets[i], *nxt;

			while (cur) {
				nxt = cur->nxt;

				free(cur);
				cur = nxt;
			}
		}
	}

	free(ht->buckets);
	free(ht);
}

/* =========================== LRU ============================== */

struct lru *lru_new(uint64_t size)
{
	struct lru *lru = xmalloc(sizeof(struct lru));
	
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
	while (lru->list->used >= lru->allow) {
		struct list_node *tail = lru->list->tail;
		struct hashtable *ht = lru->ht;

		/* free list tail */
		if (tail) {
			hashtable_remove(ht, tail->sk.data);
			list_remove(lru->list, tail);
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
	}

	node = xcalloc(1, sizeof(struct list_node));

	char *kdata= xmalloc(sk->len + 1);

	memset(kdata, 0, sk->len + 1);
	memcpy(kdata, sk->data, sk->len);
	node->sk.len = sk->len;
	node->sk.data = kdata;

	char *vdata= xmalloc(sv->len + 1);
	memset(vdata, 0, sv->len + 1);
	memcpy(vdata, sv->data, sv->len);
	node->sv.len = sv->len;
	node->sv.data = vdata;
	node->size = (sk->len + sv->len);

	hashtable_set(lru->ht, node->sk.data, node);
	list_push(lru->list, node);
}

int lru_get(struct lru *lru, struct slice *sk, struct slice *ret)
{
	struct list_node *node;

	node = hashtable_get(lru->ht, sk->data);
	if (node) {
		ret->len = node->sv.len;
		char *data = xmalloc(ret->len + 1);
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
		list_remove(lru->list, node);
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
