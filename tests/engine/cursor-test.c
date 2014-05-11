/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "tree.h"
#include "tree-func.h"
#include "cursor.h"
#include "cache.h"
#include "ctest.h"

static struct tree_callback tree_cb = {
	.fetch_node = fetch_node_callback,
	.flush_node = flush_node_callback,
	.fetch_hdr = fetch_hdr_callback,
	.flush_hdr = flush_hdr_callback
};


#define DUMMY_TXID (0UL)
#define DBPATH0 "cursor-test-0.brt"
CTEST(cursor, empty)
{
	int ret;
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	xreset();
	/* create */
	opts = options_new();
	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH0, opts, status, cache, &tree_cb);


	/* cursor */
	struct cursor *cur;

	cur = cursor_new(tree, NULL);

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
	cache_free(cache);
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

	xreset();
	//create
	opts = options_new();
	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH1, opts, status, cache, &tree_cb);

	LOGGER *logger;
	struct txn *txn1;
	struct txnmgr *txnmgr;
	int readonly = 0;

	txnmgr = txnmgr_new();
	logger = logger_new(NULL, txnmgr);
	ret = txn_begin(NULL, logger, TXN_ISO_READ_COMMITTED, readonly, &txn1);
	ASSERT_EQUAL(1, ret);

	//insert to tree
	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_INSERT, txn1);
	}
	txn_finish(txn1);


	struct txn *txn2;
	readonly = 1;
	ret = txn_begin(NULL, logger, TXN_ISO_READ_COMMITTED, readonly, &txn2);
	ASSERT_EQUAL(1, ret);

	struct cursor *cur;
	cur = cursor_new(tree, txn2);
	tree_cursor_first(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(1, ret);

	int j = 1;
	while (tree_cursor_valid(cur)) {
		tree_cursor_next(cur);
		j++;
	}
	ASSERT_EQUAL(R, j);
	tree_cursor_last(cur);
	ret = tree_cursor_valid(cur);
	ASSERT_EQUAL(1, ret);
	txn_finish(txn2);

	cursor_free(cur);

	// free
	logger_free(logger);
	txnmgr_free(txnmgr);

	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

/*
#define DBPATH2 "cursor-test-3.brt"
CTEST(cursor, onlyleaf)
{
	int i;
	int ret;
	int R = 24;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	opts = options_new();
	opts->leaf_node_page_count = 4;
	opts->inner_node_page_count = 6;

	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH2, opts, status, cache, &tree_cb);

	LOGGER *logger;
	struct txn *txn1;
	struct txnmgr *txnmgr;
	int readonly = 0;

	txnmgr = txnmgr_new();
	logger = logger_new(NULL, txnmgr);
	ret = txn_begin(NULL, logger, TXN_ISO_READ_COMMITTED, readonly, &txn1);
	ASSERT_EQUAL(1, ret);

	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_INSERT, txn1);
	}
	txn_finish(txn1);

	ASSERT_EQUAL(1, tree->hdr->height);
	ASSERT_EQUAL(4, status->tree_leaf_split_nums);

	struct cursor *cur;

	cur = cursor_new(tree);

	int c = 0;
	for (tree_cursor_first(cur); tree_cursor_valid(cur) != 0;
			tree_cursor_next(cur)) {
		c++;
	}
	ASSERT_EQUAL(R, c);

	cursor_free(cur);

	logger_free(logger);
	txnmgr_free(txnmgr);

	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define DBPATH3 "cursor-test-inner.brt"
CTEST(cursor, inner)
{
	int i;
	int ret;
	int R = 24;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	opts = options_new();
	opts->leaf_node_page_count = 4;
	opts->inner_node_page_count = 4;
	opts->inner_node_fanout = 4;

	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH3, opts, status, cache, &tree_cb);

	LOGGER *logger;
	struct txn *txn1;
	struct txnmgr *txnmgr;
	int readonly = 0;

	txnmgr = txnmgr_new();
	logger = logger_new(NULL, txnmgr);
	ret = txn_begin(NULL, logger, TXN_ISO_READ_COMMITTED, readonly, &txn1);
	ASSERT_EQUAL(1, ret);

	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_INSERT, txn1);
	}

	txn_finish(txn1);

	ASSERT_EQUAL(2, tree->hdr->height);
	//ASSERT_EQUAL(5, status->tree_leaf_split_nums);

	struct cursor *cur;

	cur = cursor_new(tree);

	int c = 0;
	for (tree_cursor_first(cur); tree_cursor_valid(cur) != 0;
			tree_cursor_next(cur)) {
		c++;
	}
	ASSERT_EQUAL(R, c);

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

	cursor_free(cur);

	logger_free(logger);
	txnmgr_free(txnmgr);

	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define DBPATH4 "cursor-test-inner-large.brt"
CTEST(cursor, inner_large)
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

	opts = options_new();
	opts->leaf_node_page_count = 16;
	opts->inner_node_page_count = 16;
	opts->inner_node_fanout = 4;

	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH4, opts, status, cache, &tree_cb);

	LOGGER *logger;
	struct txn *txn1;
	struct txnmgr *txnmgr;
	int readonly = 0;

	txnmgr = txnmgr_new();
	logger = logger_new(NULL, txnmgr);
	ret = txn_begin(NULL, logger, TXN_ISO_READ_COMMITTED, readonly, &txn1);
	ASSERT_EQUAL(1, ret);

	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_INSERT, txn1);
	}
	txn_finish(txn1);

	struct cursor *cur;

	cur = cursor_new(tree);

	int c = 0;
	for (tree_cursor_first(cur); tree_cursor_valid(cur) != 0;
			tree_cursor_next(cur)) {
		c++;
		fprintf(stderr, "---tree cursor test forward done %d/%d %30s\r",
				c, R,  "");
		fflush(stderr);
	}
	ASSERT_EQUAL(R, c);

	c = 0;
	for (tree_cursor_last(cur); tree_cursor_valid(cur) != 0;
			tree_cursor_prev(cur)) {
		c++;
		fprintf(stderr, "---tree cursor test backward done %d/%d %30s\r",
				c, R, "");
		fflush(stderr);
	}
	ASSERT_EQUAL(R, c);


	cursor_free(cur);

	logger_free(logger);
	txnmgr_free(txnmgr);

	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}
*/
