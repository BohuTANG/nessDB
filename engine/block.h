#ifndef __nessDB_BLOCK_H
#define __nessDB_BLOCK_H

#include "internal.h"
#include "db.h"

struct block {
	int blks;
	int level_blks[MAX_LEVEL];
	int level_blk_used[MAX_LEVEL];
	int level_blk_aligned[MAX_LEVEL];
	struct sst_item *blocks[MAX_LEVEL];
};

struct block *block_new(int l0_count);
void block_build(struct block *block, struct sst_item *item,
				int count, int level);
int block_search(struct block *block, struct slice *sk, int level);
void block_reset(struct block *block, int level);
void block_free(struct block *block);

#endif
