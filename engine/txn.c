/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "txn.h"
#include "txnmgr.h"
#include "debug.h"

int txn_begin(TXN *parent, LOGGER *logger, TXN_ISOLATION_TYPE iso, TXN **txn)
{
	struct txn *tx;
	struct txnmgr *txnmgr = logger->txnmgr;
	
	tx = xcalloc(1, sizeof(*tx));
	tx->logger = logger;
	tx->iso_type = iso;

	switch (tx->iso_type) {
	case TXN_ISO_SERIALIZABLE:
	case TXN_ISO_READ_UNCOMMITTED:
		tx->snapshot_type = TXN_SNAPSHOT_NONE;
		break;
	case TXN_ISO_REPEATABLE:
		tx->snapshot_type = TXN_SNAPSHOT_ROOT;
		break;
	case TXN_ISO_READ_COMMITTED:
		tx->snapshot_type = TXN_SNAPSHOT_CHILD;
		break;
	default:
		__PANIC("unknow txn isolation type, %d", tx->iso_type);
		break;
	}

	if (parent == NULL) {
		txnmgr_txn_start(txnmgr, tx);
	} else {
		txnmgr_child_txn_start(txnmgr, parent, tx);
	}
	*txn = tx;

	return NESS_OK;
}

int txn_commit(TXN *txn)
{
	(void)txn;

	/* TODO:(BohuTANG)
	 * 1) commit rollentry
	 * 2) free rollentry
	 */

	return NESS_OK;
}

int txn_abort(TXN *txn)
{
	(void)txn;

	/* TODO:(BohuTANG)
	 * 1) free rollentry
	 */

	return NESS_OK;
}
