/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MEMPOOL_H_
#define nessDB_MEMPOOL_H_

#include "internal.h"

/**
 * @file mempool.h
 * @brief mempool definitions
 *
 * a simple memory pool:
 *	- 4KB per block
 *	- memory is immutable after alloced
 *	- aligned address
 */

struct mempool_block {
	char *memory;
	uint32_t size;
	uint32_t remaining;
	struct mempool_block *next;
};

struct mempool {
	uint32_t n_blocks;
	uint32_t memory_size;
	uint32_t memory_used;
	struct mempool_block *blocks;
};

struct mempool *mempool_new();
char *mempool_alloc_aligned(struct mempool *pool, uint32_t bytes);
void mempool_free(struct mempool *pool);

#endif /* nessDB_MEMPOOL_H_ */
