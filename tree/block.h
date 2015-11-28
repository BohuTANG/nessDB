/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_BLOCK_H_
#define nessDB_BLOCK_H_

/**
 * @file block.h
 * @brief block translation table
 *
 * key ideas in block are:
 * node nid ---> disk offset
 *
 */

/* reserved 1024bytes for double-write tree header */
#define BLOCK_OFFSET_START (ALIGN(1024))

struct block_pair {
	DISKOFF offset;
	NID nid;
	uint32_t real_size;
	uint32_t skeleton_size;
	uint32_t height;
	uint8_t used;
};

struct block {
	DISKOFF allocated;
	uint32_t pairs_used;
	uint32_t pairs_size;
	struct block_pair * pairs;
	struct status *status;
	ness_mutex_t mtx;
	struct rwlock rwlock;
};

struct block *block_new();
void block_init(struct block *b,
                struct block_pair *pairs,
                uint32_t size);

DISKOFF block_alloc_off(struct block *b,
                        NID nid,
                        uint32_t real_size,
                        uint32_t skeleton_size,
                        uint32_t height);

DISKOFF block_realloc_off(struct block *b,
                          NID nid,
                          uint32_t real_size,
                          uint32_t skeleton_size,
                          uint32_t height);

int block_get_off_bynid(struct block *b,
                        NID nid,
                        struct block_pair **pair);

void block_shrink(struct block *b);
void block_free(struct block *b);

#endif /* nessDB_BLOCK_H_ */
