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
#include "ctest.h"

CTEST(txn, begin_with_no_parent)
{
	int r;
	struct txn *tx1;
	struct txn *tx2;
	struct txn *tx3;
	struct txnmgr *tm = txnmgr_new();
	LOGGER *logger = logger_new(NULL, tm);

	/* transaction 1 */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, &tx1);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, tx1->txnid);
	ASSERT_EQUAL(1, tm->live_root_txnids->used);
	ASSERT_EQUAL(tx1->txnid, tm->live_root_txnids->txnids[0]);

	/* transaction 2 */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, &tx2);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(1, tx2->txnid);
	ASSERT_EQUAL(tx1->txnid, tx2->txnid_clone->txnids[0]);
	ASSERT_EQUAL(tx2->txnid, tx2->txnid_clone->txnids[1]);

	/* transaction 3 */
	r = txn_begin(NULL, logger, TXN_ISO_SERIALIZABLE, &tx3);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(2, tx3->txnid);
	ASSERT_NULL(tx3->txnid_clone);

	logger_free(logger);
	txnmgr_free(tm);
}

CTEST(txn, begin_with_parent)
{
	int r;
	struct txn *tx1;
	struct txn *tx2;
	struct txn *tx3;
	struct txn *tx4;
	struct txnmgr *tm = txnmgr_new();
	LOGGER *logger = logger_new(NULL, tm);

	/* tx1: snapshot */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, &tx1);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, tx1->txnid);
	ASSERT_EQUAL(1, tm->live_root_txnids->used);
	ASSERT_EQUAL(1, tx1->txnid_clone->used);
	ASSERT_EQUAL(tx1->txnid, tm->live_root_txnids->txnids[0]);

	/* tx2: snapshot */
	r = txn_begin(NULL, logger, TXN_ISO_REPEATABLE, &tx2);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(1, tx2->txnid);
	ASSERT_EQUAL(2, tx2->txnid_clone->used);
	ASSERT_EQUAL(tx1->txnid, tx2->txnid_clone->txnids[0]);
	ASSERT_EQUAL(tx2->txnid, tx2->txnid_clone->txnids[1]);

	/* tx3: not snapshot */
	r = txn_begin(NULL, logger, TXN_ISO_SERIALIZABLE, &tx3);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(2, tx3->txnid);
	ASSERT_NULL(tx3->txnid_clone);

	/* tx4: not snapshot with parent tx2 */
	r = txn_begin(tx2, logger, TXN_ISO_REPEATABLE, &tx4);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(3, tx4->txnid);
	ASSERT_EQUAL(2, tx4->txnid_clone->used);
	ASSERT_EQUAL(tx1->txnid, tx4->txnid_clone->txnids[0]);
	ASSERT_EQUAL(tx2->txnid, tx4->txnid_clone->txnids[1]);

	logger_free(logger);
	txnmgr_free(tm);
}
