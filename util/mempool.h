/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_MEMPOOL_H_
#define nessDB_MEMPOOL_H_

/**
 * @file mempool.h
 * @brief mempool definitions
 *
 * a simple memory pool:
 *	- 4KB per block
 *	- memory is immutable after alloced
 *	- aligned address
 */

struct memblk {
	char *base;
	uint32_t size;
	uint32_t remaining;
};

struct mempool {
	int blk_used;
	int blk_nums;
	uint32_t memory_size;
	uint32_t memory_used;
	struct memblk **blks;
};

struct mempool *mempool_new(void);
char *mempool_alloc(struct mempool *pool, uint32_t bytes);
char *mempool_alloc_aligned(struct mempool *pool, uint32_t bytes);
void mempool_free(struct mempool *pool);

#endif /* nessDB_MEMPOOL_H_ */
