/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _HT_H
#define _HT_H

#include <stdint.h>

struct ht{
	size_t size;
	size_t cap;
	size_t (*hashfunc)  (const char *k);
	struct ht_node **nodes;
};

struct ht_node{
	void *key;
	void *value;
	struct ht_node *next;
};

struct ht *ht_new(size_t capacity);
void ht_set(struct ht *ht,void *key,void* value);
void *ht_get(struct ht *ht,void *key);
void ht_remove(struct ht *ht,void *key);
void ht_free(struct ht *ht);

#endif
