/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"

#define MEMPOOL_BLOCKNUM	(16)
#define MEMPOOL_BLOCKSIZE	(4096*16)

struct memblk *_new_blk(struct mempool *pool, uint32_t bytes)
{
	uint32_t sizes;
	struct memblk *blk;

	sizes = (bytes > MEMPOOL_BLOCKSIZE ? bytes : MEMPOOL_BLOCKSIZE);
	blk = xcalloc(1, sizeof(*blk));
	blk->base = xmalloc(sizes * sizeof(char));
	blk->size = sizes;
	blk->remaining = sizes;
	pool->memory_size += sizes;

	return blk;
}

static inline void _add_blk(struct mempool *pool, struct memblk *blk)
{
	pool->blks[pool->blk_used++] = blk;
}

void _mempool_makeroom(struct mempool *pool)
{
	if (pool->blk_nums > (pool->blk_used))
		return;

	int new_nums = pool->blk_nums * 2;

	pool->blks = xrealloc(pool->blks, sizeof(*pool->blks) * new_nums);
	pool->blk_nums = new_nums;
}

struct mempool *mempool_new()
{
	struct mempool *pool;

	pool = xcalloc(1, sizeof(*pool));
	pool->blk_nums = MEMPOOL_BLOCKNUM;
	pool->blks = xcalloc(MEMPOOL_BLOCKNUM, sizeof(*pool->blks));
	pool->blks[pool->blk_used++] = _new_blk(pool, MEMPOOL_BLOCKSIZE);

	return pool;
}

/*
 * malloc bytes with unaligned address
 * for fifo packed memory
 */
char *mempool_alloc(struct mempool *pool, uint32_t bytes)
{
	char *base;
	char *results;
	struct memblk *blk;


	blk = pool->blks[pool->blk_used - 1];
	base = (blk->base + (blk->size - blk->remaining));

	if (blk->remaining >= bytes) {
		results = base;
	} else {
		_mempool_makeroom(pool);
		blk = _new_blk(pool, bytes);
		_add_blk(pool, blk);

		results = blk->base;
	}

	blk->remaining -= bytes;
	pool->memory_used += bytes;

	return results;
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
	struct memblk *blk;


	align = ((sizeof(void*) > 8) ? sizeof(void*) : 8);

	/* align must power of 2 */
	nassert((align & (align - 1)) == 0);

	blk = pool->blks[pool->blk_used - 1];
	base = (blk->base + (blk->size - blk->remaining));
	ptr = (size_t)base;
	mod = ptr & (align - 1);
	slob = ((mod == 0) ? 0 : (align - mod));
	needed = (bytes + slob);

	if (blk->remaining >= needed) {
		results = (base + slob);
		blk->remaining -= needed;
		pool->memory_used += needed;
	} else {
		_mempool_makeroom(pool);
		blk = _new_blk(pool, bytes);
		_add_blk(pool, blk);

		blk->remaining -= bytes;
		pool->memory_used += bytes;
		results = blk->base;
	}

	return results;
}

void mempool_free(struct mempool *pool)
{
	if (!pool)
		return;

	int i;

	for (i = 0; i < pool->blk_used; i++) {
		xfree(pool->blks[i]->base);
		xfree(pool->blks[i]);
	}

	xfree(pool->blks);
	xfree(pool);
}
