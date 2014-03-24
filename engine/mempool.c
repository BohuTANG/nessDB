/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "debug.h"
#include "mempool.h"

/* 4KB per block */
#define MEMPOOL_BLOCKSIZE	(4096)

struct mempool_block *_new_block(struct mempool *pool, uint32_t bytes) {
	uint32_t sizes;
	struct mempool_block *blk;

	sizes = (bytes > MEMPOOL_BLOCKSIZE ? bytes : MEMPOOL_BLOCKSIZE);
	blk = xcalloc(1, sizeof(*blk));
	blk->memory = xmalloc(sizes * sizeof(char));
	blk->size = sizes;
	blk->remaining = MEMPOOL_BLOCKSIZE;

	pool->memory_size += sizes;

	return blk;
}

struct mempool *mempool_new() {
	struct mempool *pool;

	pool = xcalloc(1, sizeof(*pool));
	pool->blocks = _new_block(pool, MEMPOOL_BLOCKSIZE);
	pool->n_blocks++;

	return pool;
}

/*
 * malloc bytes with aligned address
 */
char *mempool_alloc_aligned(struct mempool *pool, uint32_t bytes)
{
	int mod;
	int slob;
	int align;
	size_t ptr;

	char *base;
	char *results;
	uint32_t needed;
	struct mempool_block *blk;


	align = ((sizeof(void*) > 8) ? sizeof(void*) : 8);

	/* align must power of 2 */
	nassert((align & (align - 1)) == 0);

	blk = pool->blocks;
	base = (blk->memory + (blk->size - blk->remaining));
	ptr = (size_t)base;
	mod = ptr & (align - 1);
	slob = ((mod == 0) ? 0 : (align - mod));
	needed = (bytes + slob);

	if (blk->remaining >= needed) {
		results = (base + slob);
		blk->remaining -= needed;
		pool->memory_used += needed;
	} else {
		blk = _new_block(pool, bytes);
		blk->next = pool->blocks;
		pool->blocks = blk;
		pool->n_blocks++;

		blk->remaining -= bytes;
		pool->memory_used += bytes;

		results = blk->memory;
	}

	return results;
}

void mempool_free(struct mempool *pool)
{
	uint32_t i;
	struct mempool_block *cur;
	struct mempool_block *nxt;

	if (!pool)
		return;

	cur = pool->blocks;
	for (i = 0; i < pool->n_blocks; i++) {
		nxt = cur->next;
		xfree(cur->memory);
		xfree(cur);
		cur = nxt;
	}

	xfree(pool);
}
