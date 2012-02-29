 /* This is a binary search/insert meta
 * +--------+--------+--------+--------+
 * |             end key               |
 * +--------+--------+--------+--------+
 * |          index-file name          |
 * +--------+--------+--------+--------+
 *
 *
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "meta.h"
#include "debug.h"

struct meta *meta_new()
{
	struct meta *m = malloc(sizeof(struct meta));
	m->sn = 0;
	m->size = 0;
	return m;
}

struct meta_node *meta_get(struct meta *meta, char *key)
{
	size_t left = 0, right = meta->size, i;
	while (left < right) {
		i = (right -left) / 2 +left;
		int cmp = strcmp(key, meta->nodes[i].end);
		if (cmp == 0) 
			return &meta->nodes[i];

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	i = left;
	if ( i == meta->size)
		return NULL;

	return &meta->nodes[i];
}

void meta_set(struct meta *meta, struct meta_node *node)
{
	size_t left = 0, right = meta->size;
	while (left < right) {
		size_t i = (right -left) / 2 +left;
		int cmp = strcmp(node->end, meta->nodes[i].end);
		if (cmp == 0) {
			memcpy(meta->nodes[i].end, node->end, NESSDB_MAX_KEY_SIZE);
			return ;
		}

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}

	size_t i = left;
	meta->size++;
	node->lsn = meta->size;
	memmove(&meta->nodes[i + 1], &meta->nodes[i], (meta->size - i) * META_NODE_SIZE);
	memcpy(&meta->nodes[i], node, META_NODE_SIZE);
}

void meta_set_byname(struct meta *meta, struct meta_node *node)
{
	int i;
	for (i = 0; i < meta->size; i++) {
		int cmp = strcmp(node->index_name, meta->nodes[i].index_name);
		if (cmp == 0) {
			memcpy(meta->nodes[i].end, node->end, NESSDB_MAX_KEY_SIZE);
			return ;
		}

	}
}

void meta_free(struct meta *meta)
{
	if (meta)
		free(meta);
}
