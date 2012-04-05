/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "llru.h"

#define MAXHITS (3)
#define PRIME	(16785407)
#define RATIO	(0.618)

struct llru *llru_new(size_t buffer_size)
{
	size_t size_level_new;
	size_t size_level_old;
	struct llru *lru;

	size_level_new = buffer_size * RATIO;
	size_level_old = buffer_size - size_level_new;
	
	lru = calloc(1, sizeof(struct llru));
	lru->ht = ht_new(PRIME);

	lru->level_old.allow_size = size_level_old;
	lru->level_new.allow_size = size_level_new;

	if (buffer_size > 1023)
		lru->buffer = 1;

	return lru;
}

void _llru_set_node(struct llru *lru, struct level_node *n)
{
	if (n != NULL) {
		if (n->hits == -1) {
			/* free the last one */
			if (lru->level_new.used_size >= lru->level_new.allow_size) {
				ht_remove(lru->ht, n->sk.data);
				level_free_last(&lru->level_new);
			}

			/* set n to head*/
			level_remove_link(&lru->level_new, n);
			level_set_head(&lru->level_new, n);
		} else {
			if (lru->level_old.used_size >= lru->level_old.allow_size) {
				ht_remove(lru->ht, n->sk.data);
				level_free_last(&lru->level_old);
			}

			n->hits++;
			level_remove_link(&lru->level_old, n);
			if (n->hits > MAXHITS) {
				level_set_head(&lru->level_new, n);
				n->hits = -1;
				lru->level_old.used_size -= n->size;
				lru->level_new.used_size += n->size;
			} else
				level_set_head(&lru->level_old, n);
		}
	}
}

void llru_set(struct llru *lru, struct slice *sk, struct slice *sv)
{
	size_t size;
	struct level_node *n;

	if (lru->buffer == 0)
		return;

	size = (sk->len + sv->len);
	n = (struct level_node*)ht_get(lru->ht, sk->data);
	if (n == NULL) {
		lru->level_old.used_size += size;
		lru->level_old.count++;

		n = calloc(1, sizeof(struct level_node));
		n->sk.data = malloc(sk->len);
		n->sk.len = sk->len;
		memset(n->sk.data, 0, sk->len);
		memcpy(n->sk.data, sk->data, sk->len);

		n->sv.data = malloc(sv->len);
		n->sv.len = sv->len;
		memset(n->sv.data, 0, sv->len);
		memcpy(n->sv.data, sv->data, sv->len);

		n->size = size;
		n->hits = 1;
		n->pre = NULL;
		n->nxt = NULL;

		ht_set(lru->ht, n->sk.data, n);
	}

	_llru_set_node(lru, n);
}

struct slice *llru_get(struct llru *lru, struct slice *sk)
{
	struct level_node *n;

	if (lru->buffer == 0)
		return NULL;

	n = (struct level_node*)ht_get(lru->ht, sk->data);
	if (n != NULL) {
		_llru_set_node(lru, n);
		return &n->sv;
	}

	return NULL;
}

void llru_remove(struct llru *lru, struct slice *sk)
{
	struct level_node *n;

	if (lru->buffer == 0)
		return;

	n = (struct level_node *)ht_get(lru->ht, sk->data);
	if (n != NULL) {
		ht_remove(lru->ht, sk->data);
		if (n->hits == -1) 
			level_free_node(&lru->level_new, n);
		else 
			level_free_node(&lru->level_old, n);
	}
}

void llru_free(struct llru *lru)
{
	ht_free(lru->ht);
	free(lru);
}

