/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "internal.h"
#include "block.h"
#include "ctest.h"

CTEST(block_test, block_get) {
	struct block *b = block_new();

	uint32_t height = 0;
	uint32_t skeleton_size = 0;

	uint64_t nid0 = 0;
	DISKOFF off0 = block_alloc_off(b, nid0, 513, skeleton_size, height);

	/* {1024 to 2048} */
	ASSERT_EQUAL(1024, off0);

	uint64_t nid1 = 1;
	off0 = block_alloc_off(b, nid1, 513, skeleton_size, height);

	/* {2048 to 3072} */
	ASSERT_EQUAL(2048, off0);

	uint64_t nid2 = 2;
	off0 = block_alloc_off(b, nid2, 512, skeleton_size, height);

	/* {3072 to 3584} */
	ASSERT_EQUAL(3072, off0);

	uint64_t nid3 = 3;
	off0 = block_alloc_off(b, nid3, 511, skeleton_size, height);

	/* {3584 to 4096} */
	ASSERT_EQUAL(3584, off0);

	/* realloc nid1 */
	nid1 = 1;
	off0 = block_alloc_off(b, nid1, 513, skeleton_size, height);

	/*
	 * {4096 to 5120} is used
	 * {2048 to 3072} is unused
	 */
	ASSERT_EQUAL(4096, off0);

	/* realloc nid2 */
	nid2 = 2;
	off0 = block_alloc_off(b, nid2, 512, skeleton_size, height);

	/*
	 * {5120 to 5632} used
	 * {3072 to 3584} is unused
	 */
	ASSERT_EQUAL(5120, off0);

	block_shrink(b);

	uint64_t nid4 = 4;
	off0 = block_alloc_off(b, nid4, 511, skeleton_size, height);
	ASSERT_EQUAL(2048, off0);

	uint64_t nid5 = 5;
	off0 = block_alloc_off(b, nid5, 511, skeleton_size, height);
	ASSERT_EQUAL(2560, off0);

	block_free(b);
}
