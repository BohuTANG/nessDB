/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "posix.h"
#include "mempool.h"
#include "debug.h"
#include "skiplist.h"
#include "ctest.h"

int _compare_fun(void *a, void *b)
{
	if (!a)
		return -1;
	if (!b)
		return +1;

	int ia = *((int*)a);
	int ib = *((int*)b);

	if (ia < ib)
		return -1;
	else if (ia > ib)
		return +1;
	else
		return 0;
}

judgetype_t _judge_fun(void *a, void *b)
{
	if (!a || !b)
		return J_PUT;

	int ia = *((int*)a);
	int ib = *((int*)b);

	if (ia == ib)
		return J_OVERWRITE;

	return J_PUT;
}

CTEST(skiplist, empty)
{
	int ret;
	int k = 10;

	struct mempool *mpool = mempool_new();
	struct skiplist *list = skiplist_new(mpool,
			&_judge_fun,
			&_compare_fun);
	struct skiplist_iter iter;

	skiplist_iter_init(&iter, list);
	skiplist_iter_seek(&iter, &k);

	ret = skiplist_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	skiplist_iter_seektofirst(&iter);
	ret = skiplist_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	k = 100;
	skiplist_iter_seek(&iter, &k);
	ret = skiplist_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	skiplist_iter_seektolast(&iter);
	ret = skiplist_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	mempool_free(mpool);
	skiplist_free(list);
	xcheck_all_free();
}

CTEST(skiplist, insert_and_lookup)
{
	int i;
	int N = 5000;
	int R = 2000;
	int keys[N];
	int holes[N];

	struct skiplist_iter iter;
	struct mempool *mpool = mempool_new();
	struct skiplist *list = skiplist_new(mpool,
			&_judge_fun,
			&_compare_fun);

	for (i = 0; i < N; i++) {
		keys[i] = i;
		holes[i] = 0;
	}

	for (i = 0; i < R; i++) {
		int idx = rand() % N;
		while (holes[idx]) {
			idx = rand() % N;
		}
		skiplist_put(list, &keys[idx]);
		holes[idx] = 1;
	}

	int k = 0;
	skiplist_iter_init(&iter, list);
	skiplist_iter_seek(&iter, &k);
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));

	for (i = 0; i < N; i++) {
		if (holes[i])
			break;
	}
	
	ASSERT_EQUAL(i, (int)*(int*)iter.node->key);

	mempool_free(mpool);
	skiplist_free(list);
	xcheck_all_free();
}

CTEST(skiplist, insert_same_key)
{
	struct mempool *mpool = mempool_new();
	struct skiplist *list = skiplist_new(mpool,
			&_judge_fun,
			&_compare_fun);

	int ret;
	int k = 2014;

	skiplist_put(list, &k);

	struct skiplist_iter iter;
	skiplist_iter_init(&iter, list);
	skiplist_iter_seek(&iter, &k);
	ret = skiplist_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);

	/* same key put test */
	skiplist_put(list, &k);

	mempool_free(mpool);
	skiplist_free(list);
	xcheck_all_free();
}

CTEST(skiplsit, concurrent)
{
	//Waiting
}
