/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Block is some like fractional cascading.
 * Here using sketch algorithm to speed up SST level's search, search costs is O(log N)
 */

#include "block.h"
#include "sst.h"
#include "xmalloc.h"
#include "debug.h"

struct block *block_new(int l0_blk_count)
{
	int i;
	struct block *block = xcalloc(1, sizeof(struct block));

	for (i = 0; i < (int)MAX_LEVEL; i++) {
		block->level_blks[i] = (pow(LEVEL_BASE, i) * l0_blk_count);
		block->blocks[i] = xcalloc(block->level_blks[i], ITEM_SIZE);
		block->blks += block->level_blks[i];
	}

	return block;
}

void block_build(struct block *block, struct sst_item *items, int count, int level)
{
	int i;
	int x = count / BLOCK_GAP;
	int mod = count % BLOCK_GAP;

	if (count == 0)
		return;

	memset(block->blocks[level], 0, block->level_blks[level] * ITEM_SIZE);
	block->level_blk_used[level] = 0;
	for (i = 0; i < x; i++) {
		memcpy(&block->blocks[level][i], &items[i * BLOCK_GAP], ITEM_SIZE);
		block->level_blk_used[level]++;
	}

	if (mod > 0) {
		memcpy(&block->blocks[level][i], &items[count - mod], ITEM_SIZE);
		block->level_blk_used[level]++;
	}
}

int block_search(struct block *block, const char *key, int level)
{
	int i;
	int cmp;
	int used = block->level_blk_used[level];

	for (i = 0; i < used; i++) {
		cmp = strcmp(key, block->blocks[level][i].data);
		if (cmp == 0)
			goto RET;

		if (cmp < 0) {
			i -= 1;
			goto RET;
		}
	}

RET:
	if (i == used)
		i -= 1;

	return i;
}

void block_free(struct block *block)
{
	int i;

	for (i = 0; i < (int)MAX_LEVEL; i++) 
		free(block->blocks[i]);

	free(block);
}
