/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Block is some like fractional cascading.
 * Here using sketch algorithm to speed up SST level's search, search costs is O(log N)
 */

#include "block.h"
#include "xmalloc.h"
#include "debug.h"

int _calc_flr(struct block *block, int level) 
{
	int i = 0;
	int flr = 0;

	for (i = 0; i < level; i++)
		flr += block->level_blks[i];

	return flr;
}

void _block_dump(struct block *block, int level)
{
	int i;
	int flr = _calc_flr(block, level);
	int used = block->level_blk_used[level];

	printf("---->block dump[used#%d, level#%d, count:%d]:\r\n\t", used, level, block->level_blks[level]);
	for (i = 0; i < used; i++) {
		printf(" %s ", block->blocks[flr + i].data);
	}
	printf("\r\n");
}

struct block *block_new(int l0_blk_count)
{
	int i;
	struct block *block = xcalloc(1, sizeof(struct block));

	for (i = 0; i < (int)MAX_LEVEL; i++) {
		/* aligned five gaps */
		block->level_blks[i] = ((1<<i) * l0_blk_count + 100);
		block->blks += block->level_blks[i];
	}
	block->blocks = xcalloc(1 + block->blks, ITEM_SIZE);

	return block;
}

void block_build(struct block *block, struct cola_item *items, int count, int level)
{
	int i;
	int x = count / BLOCK_GAP;
	int mod = count % BLOCK_GAP;
	int flr = _calc_flr(block, level);

	memset(&block->blocks[flr], 0, block->level_blks[level] * ITEM_SIZE);
	block->level_blk_used[level] = 0;
	for (i = 0; i < x; i++) {
		memcpy(&block->blocks[flr++], &items[i * BLOCK_GAP], ITEM_SIZE);
		block->level_blk_used[level]++;
	}

	if (mod > 0) {
		memcpy(&block->blocks[flr++], &items[count - mod], ITEM_SIZE);
		block->level_blk_used[level]++;
	}
}

int block_search(struct block *block, const char *key, int level)
{
	int i;
	int cmp;
	int flr = _calc_flr(block, level);
	int used = block->level_blk_used[level];

	for (i = 0; i < used; i++) {
		cmp = strcmp(key, block->blocks[flr + i].data);
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
	free(block->blocks);
	free(block);
}
