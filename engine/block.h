#ifndef __nessDB_BLOCK_H
#define __nessDB_BLOCK_H

#include "db.h"

struct block {
	int blks;
	int level_blks[MAX_LEVEL];
	int level_blk_used[MAX_LEVEL];
	int level_blk_aligned[MAX_LEVEL];
	struct cola_item *blocks;
};

struct block *block_new(int l0_count);
void block_build(struct block *block, struct cola_item *item, int count, int level);
int block_search(struct block *block, const char *key, int level);
void block_free(struct block *block);

#endif
