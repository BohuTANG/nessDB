/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "tree.h"
#include "tcursor.h"
#include "dbcache.h"
#include "ctest.h"

#define DUMMY_TXID (0UL)
#define DBPATH0 "cursor-test-0.brt"
CTEST(cursor, empty)
{
	int ret;
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	/* create */
	opts = options_new();
	status = status_new();
	cache = dbcache_new(opts);
	tree = tree_new(DBPATH0, opts, status, cache, 1);


	/* cursor */
	struct cursor *cur;

	cur = cursor_new(tree);

	/* first */
	tree_cursor_first(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(0, ret);

	/* last */
	tree_cursor_last(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(0, ret);

	/* next */
	tree_cursor_next(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(0, ret);

	cursor_free(cur);

	/* free */
	dbcache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define KEY_SIZE (16)
#define VAL_SIZE (100)

#define DBPATH1 "cursor-test-1.brt"
CTEST(cursor, 1leaf)
{
	int i;
	int ret;
	int R = 1000;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	/* create */
	opts = options_new();
	status = status_new();
	cache = dbcache_new(opts);
	tree = tree_new(DBPATH1, opts, status, cache, 1);

	/* insert to tree */
	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_PUT);
	}

	/* cursor */
	struct cursor *cur;

	cur = cursor_new(tree);

	/* first */
	tree_cursor_first(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(1, ret);

	/* next */
	int j;
	for (j = 1; j < R - 1; j++) {
		tree_cursor_next(cur);
		ret = tree_cursor_valid(cur);
		ASSERT_EQUAL(1, ret);
	}

	/* last */
	tree_cursor_last(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(1, ret);

	cursor_free(cur);

	/* free */
	dbcache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define DBPATH2 "cursor-test-3.brt"
CTEST(cursor, onlyleaf)
{
	int i;
	int R = 24;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	/* create */
	opts = options_new();
	opts->leaf_node_page_count = 4;
	opts->inner_node_page_count = 6;

	status = status_new();
	cache = dbcache_new(opts);
	tree = tree_new(DBPATH2, opts, status, cache, 2);

	/* insert to tree */
	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_PUT);
	}

	/* check leaf split*/
	ASSERT_EQUAL(1, tree->hdr->height);
	ASSERT_EQUAL(4, status->tree_leaf_split_nums);

	/* cursor */
	struct cursor *cur;

	cur = cursor_new(tree);

	int c = 0;
	for (tree_cursor_first(cur); tree_cursor_valid(cur) != 0; tree_cursor_next(cur)) {
		c++;
	}
	ASSERT_EQUAL(R, c);

	/* cursor slide test */
	/*
	tree_cursor_first(cur);
	printf("---first key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_prev(cur);
	printf("---prev key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);
	*/

	cursor_free(cur);

	/* free */
	dbcache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define DBPATH3 "cursor-test-inner.brt"
CTEST(cursor, inner)
{
	int i;
	int R = 24;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	/* create */
	opts = options_new();
	opts->leaf_node_page_count = 4;
	opts->inner_node_page_count = 4;
	opts->inner_node_fanout = 4;

	status = status_new();
	cache = dbcache_new(opts);
	tree = tree_new(DBPATH3, opts, status, cache, 1);

	/* insert to tree */
	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_PUT);
	}

	/* check leaf split*/
	ASSERT_EQUAL(2, tree->hdr->height);
	//ASSERT_EQUAL(5, status->tree_leaf_split_nums);

	/* cursor */
	struct cursor *cur;

	cur = cursor_new(tree);

	int c = 0;
	for (tree_cursor_first(cur); tree_cursor_valid(cur) != 0; tree_cursor_next(cur)) {
		c++;
	}
	ASSERT_EQUAL(R, c);

	/* cursor slide test */
	/*
	tree_cursor_first(cur);
	printf("---first key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_prev(cur);
	printf("---prev key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_next(cur);
	printf("---next key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);

	tree_cursor_current(cur);
	printf("---current key %s, %s\n", (char*)cur->key.data, (char*)cur->val.data);
	*/

	cursor_free(cur);

	/* free */
	dbcache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define DBPATH4 "cursor-test-inner-large.brt"
CTEST(cursor, inner_large)
{
	int i;
	int R = 1000;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	/* create */
	opts = options_new();
	opts->leaf_node_page_count = 16;
	opts->inner_node_page_count = 16;
	opts->inner_node_fanout = 4;

	status = status_new();
	cache = dbcache_new(opts);
	tree = tree_new(DBPATH4, opts, status, cache, 1);

	/* insert to tree */
	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_PUT);
	}

	/* cursor */
	struct cursor *cur;

	cur = cursor_new(tree);

	int c = 0;
	for (tree_cursor_first(cur); tree_cursor_valid(cur) != 0; tree_cursor_next(cur)) {
		c++;
		fprintf(stderr, "---tree cursor test forward done %d/%d %30s\r", c, R,  "");
		fflush(stderr);
	}
	ASSERT_EQUAL(R, c);

	c = 0;
	for (tree_cursor_last(cur); tree_cursor_valid(cur) != 0; tree_cursor_prev(cur)) {
		c++;
		fprintf(stderr, "---tree cursor test backward done %d/%d %30s\r", c, R, "");
		fflush(stderr);
	}
	ASSERT_EQUAL(R, c);


	cursor_free(cur);

	/* free */
	dbcache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}
