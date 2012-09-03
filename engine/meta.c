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
	struct meta *m = calloc(1, sizeof(struct meta));

	if (!m)
		__PANIC("meta new is NULL....,will be abort");

	return m;
}

struct meta_node *meta_get(struct meta *meta, char *key)
{
	int i = 0;

	for ( ; i < meta->size; i++) {
		int cmp = strcmp(key, meta->nodes[i].end);

		if (cmp == 0) 
			return &meta->nodes[i];

		if (cmp < 0)
			break;
	}

	if (i == meta->size)
		return NULL;

	return &meta->nodes[i];
}

void meta_set(struct meta *meta, struct meta_node *node)
{
	int i = 0;

	if (meta->size == (META_NODE_SIZE -1)) {
		__ERROR("too many metas, %d", meta->size);
		return;
	}

	if (meta->size == 0) {
		memcpy(&meta->nodes[0], node, META_NODE_SIZE);
		goto RET;
	}

	for ( ; i < meta->size; i++) {
		int cmp = strcmp(node->end, meta->nodes[i].end);

		if (cmp < 0) {
			break;
		}
	}

	memmove(&meta->nodes[i + 1], &meta->nodes[i], (meta->size - i) * META_NODE_SIZE);
	memcpy(&meta->nodes[i], node, META_NODE_SIZE);

RET:
	meta->size++;
	meta->nodes[i].lsn = meta->size;
}

void meta_set_byname(struct meta *meta, struct meta_node *node)
{
	int i = 0;

	for ( ; i < meta->size; i++) {
		int cmp = strcmp(node->name, meta->nodes[i].name);

		if (cmp == 0) {
			meta->nodes[i].count = node->count;
			memcpy(meta->nodes[i].end, node->end, NESSDB_MAX_KEY_SIZE);
			return ;
		}

	}
}

void meta_dump(struct meta *meta)
{
	int i;
	printf("--Meta dump:count<%d>\n", meta->size);
	for (i = 0; i< meta->size; i++) {
		struct meta_node n = meta->nodes[i];
		printf("\t(%d) end:<%s>,indexname:<%s>,hascount:<%d>,lsn:<%d>\n",
				i,
				n.end,
				n.name,
				n.count,
				n.lsn);
	}
}

void meta_free(struct meta *meta)
{
	if (meta)
		free(meta);
}
