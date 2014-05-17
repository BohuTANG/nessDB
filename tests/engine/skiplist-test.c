/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "posix.h"
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
	else {
		return 0;
	}
}


CTEST(skiplist, empty)
{
	int ret;
	int k = 10;

	struct skiplist *list = skiplist_new(&_compare_fun);
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
	struct skiplist *list = skiplist_new(&_compare_fun);

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
		if (holes[i]) {
			int exp;

			skiplist_iter_seek(&iter, &i);
			ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
			exp = *(int*)iter.node->key;
			ASSERT_EQUAL(i, exp);
		}
	}

	skiplist_free(list);
	xcheck_all_free();
}

CTEST(skiplist, insert_same_key)
{
	struct skiplist *list = skiplist_new(&_compare_fun);

	int ret;
	int k = 2014;
	struct skiplist_iter iter;

	skiplist_iter_init(&iter, list);

	skiplist_put(list, &k);
	skiplist_put(list, &k);
	skiplist_put(list, &k);
	skiplist_put(list, &k);
	skiplist_put(list, &k);

	skiplist_iter_seek(&iter, &k);
	ret = skiplist_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);

	ASSERT_EQUAL(k, *(int*)iter.node->key);

	skiplist_free(list);
	xcheck_all_free();
}

CTEST(skiplist, cursor)
{
	int i;
	int N = 5000;
	int keys[N];

	struct skiplist_iter iter;
	struct skiplist *list = skiplist_new(&_compare_fun);

	for (i = 0; i < N; i++) {
		keys[i] = i;
		skiplist_put(list, &keys[i]);
	}

	skiplist_iter_init(&iter, list);
	skiplist_iter_seektofirst(&iter);
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
	ASSERT_EQUAL(0, *(int*)iter.node->key);

	skiplist_iter_seektolast(&iter);
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
	ASSERT_EQUAL(N - 1, *(int*)iter.node->key);

	i = 1982;
	skiplist_put(list, &keys[i]);
	skiplist_put(list, &keys[i]);

	skiplist_iter_seek(&iter, &keys[i]);
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
	ASSERT_EQUAL(i, *(int*)iter.node->key);

	int s = 1981;
	skiplist_iter_prev(&iter);
	while (skiplist_iter_valid(&iter) && (s > 1500)) {
		ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
		ASSERT_EQUAL(s, *(int*)iter.node->key);
		skiplist_iter_prev(&iter);
		s -= 1;
	}

	skiplist_iter_next(&iter);
	s = 1501;
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
	while (skiplist_iter_valid(&iter) && (s < 1981)) {
		ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
		ASSERT_EQUAL(s, *(int*)iter.node->key);
		skiplist_iter_next(&iter);
		s += 1;
	}

	skiplist_iter_next(&iter);
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
	ASSERT_EQUAL(i, *(int*)iter.node->key);
	ASSERT_EQUAL(i, *(int*)iter.node->key);

	skiplist_iter_next(&iter);
	ASSERT_EQUAL(1, skiplist_iter_valid(&iter));
	ASSERT_EQUAL(i + 1, *(int*)iter.node->key);

	skiplist_free(list);
	xcheck_all_free();
}

CTEST(skiplist, benchmark)
{
	int i;
	int N = 5000000;
	int *keys;
	long long cost_ms;
	struct timespec start, end;
	struct skiplist *list = skiplist_new(&_compare_fun);

	keys = xcalloc(N, sizeof(int));
	gettime(&start);
	for (i = 0; i < N; i++) {
		keys[i] = i;
		skiplist_put(list, &keys[i]);
		if ((i % 100000) == 0) {
			fprintf(stderr, "write finished %d, used %.f MB ops%30s\r",
			        i,
			        (double)list->mpool->memory_used / 1048576,
			        "");

			fflush(stderr);
		}
	}
	gettime(&end);
	cost_ms = time_diff_ms(start, end);
	printf("--------loop:%d, cost:%d(ms), %.f ops/sec, %.fMB memory",
	       N,
	       (int)cost_ms,
	       (double)(N / cost_ms) * 1000,
	       (double)list->mpool->memory_used / 1048576
	      );


	skiplist_free(list);
	xfree(keys);
	xcheck_all_free();
}

void _do_write(struct skiplist *list, int *keys, int n)
{
	int i;

	for (i = 0; i < n; i++) {
		keys[i] = i;
		skiplist_put(list, &keys[i]);
		if (i % 200000 == 0)
			fprintf(stderr, " run %d of %d\n", i, n);
	}
}

void _do_prev(struct skiplist *list)
{
	struct skiplist_iter iter;

	skiplist_iter_init(&iter, list);
	skiplist_iter_seektolast(&iter);

	while (skiplist_iter_valid(&iter)) {
		skiplist_iter_prev(&iter);
	}
}

void _do_next(struct skiplist *list)
{
	struct skiplist_iter iter;

	skiplist_iter_init(&iter, list);
	skiplist_iter_seektofirst(&iter);

	while (skiplist_iter_valid(&iter)) {
		skiplist_iter_next(&iter);
	}
}
