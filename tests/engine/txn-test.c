/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "xtypes.h"
#include "txn.h"
#include "txnmgr.h"
#include "logger.h"
#include "tree-func.h"
#include "ctest.h"

CTEST(txn, begin_with_no_parent)
{
	int r;
	int readonly = 0;
	struct txn *tx1;
	struct txn *tx2;
	struct txn *tx3;
	struct txnmgr *tm = txnmgr_new();
	LOGGER *logger = logger_new(NULL, tm);

	/* transaction 1 */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, readonly, &tx1);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, tx1->txnid);
	ASSERT_EQUAL(1, tm->live_root_txnids->used);
	ASSERT_EQUAL(tx1->txnid, tm->live_root_txnids->txnids[0]);

	/* transaction 2 */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, readonly, &tx2);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(1, tx2->txnid);
	ASSERT_EQUAL(tx1->txnid, tx2->txnid_clone->txnids[0]);
	ASSERT_EQUAL(tx2->txnid, tx2->txnid_clone->txnids[1]);

	/* transaction 3 */
	r = txn_begin(NULL, logger, TXN_ISO_SERIALIZABLE, readonly, &tx3);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(2, tx3->txnid);
	ASSERT_NULL(tx3->txnid_clone);

	logger_free(logger);
	txnmgr_free(tm);
}

CTEST(txn, begin_with_parent)
{
	int r;
	int readonly = 0;
	struct txn *tx1;
	struct txn *tx2;
	struct txn *tx3;
	struct txn *tx4;
	struct txnmgr *tm = txnmgr_new();
	LOGGER *logger = logger_new(NULL, tm);

	/* tx1: snapshot */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, readonly, &tx1);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, tx1->txnid);
	ASSERT_EQUAL(1, tm->live_root_txnids->used);
	ASSERT_EQUAL(1, tx1->txnid_clone->used);
	ASSERT_EQUAL(tx1->txnid, tm->live_root_txnids->txnids[0]);

	/* tx2: snapshot */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, readonly, &tx2);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(1, tx2->txnid);
	ASSERT_EQUAL(2, tx2->txnid_clone->used);
	ASSERT_EQUAL(tx1->txnid, tx2->txnid_clone->txnids[0]);
	ASSERT_EQUAL(tx2->txnid, tx2->txnid_clone->txnids[1]);

	/* tx3: not snapshot */
	r = txn_begin(NULL, logger, TXN_ISO_SERIALIZABLE, readonly, &tx3);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(2, tx3->txnid);
	ASSERT_NULL(tx3->txnid_clone);

	/* tx4: not snapshot with parent tx2 */
	r = txn_begin(tx2, logger, TXN_ISO_REPEATABLE, readonly, &tx4);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(3, tx4->txnid);
	ASSERT_EQUAL(2, tx4->txnid_clone->used);
	ASSERT_EQUAL(tx1->txnid, tx4->txnid_clone->txnids[0]);
	ASSERT_EQUAL(tx2->txnid, tx4->txnid_clone->txnids[1]);

	logger_free(logger);
	txnmgr_free(tm);
}

#define KEY_SIZE (16)
#define VAL_SIZE (100)
#define DBPATH1 "txn-test-1.brt"
CTEST(txn, uncommitted)
{
	int r;
	int i;
	int R = 1000;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];

	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	LOGGER *logger;
	struct txnmgr *txnmgr;

	static struct tree_callback tree_cb = {
		.fetch_node = fetch_node_callback,
		.flush_node = flush_node_callback,
		.fetch_hdr = fetch_hdr_callback,
		.flush_hdr = flush_hdr_callback
	};

	xreset();
	/* create */
	opts = options_new();
	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH1, opts, status, cache, &tree_cb);

	txnmgr = txnmgr_new();
	logger = logger_new(NULL, txnmgr);

	/* uncommiited test */
	struct txn *tx1;
	int readonly = 0;

	r = txn_begin(NULL, logger, TXN_ISO_READ_UNCOMMITTED, readonly, &tx1);

	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, tx1->txnid);
	ASSERT_EQUAL(1, txnmgr->live_root_txnids->used);
	ASSERT_EQUAL(tx1->txnid, txnmgr->live_root_txnids->txnids[0]);

	/* insert to tree */
	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_INSERT, tx1);
	}

	txn_finish(tx1);
	/* free */
	logger_free(logger);
	txnmgr_free(txnmgr);

	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);

	xcheck_all_free();
}
