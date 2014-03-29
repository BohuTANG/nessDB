/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TXN_H_
#define nessDB_TXN_H_

#include "xtypes.h"
#include "logger.h"
#include "rollback.h"

/**
 *
 * @file txn.h
 * @brief transaction
 *
 */

struct txn {
	int readonly;
	TXNID txnid;
	TXNSTATE state;
	LOGGER *logger;
	struct txn *parent;
	struct txn *child;
	struct roll_entry *rollentry;
	struct txnid_snapshot *txnid_clone;
	TXN_ISOLATION_TYPE iso_type;
	TXN_SNAPSHOT_TYPE snapshot_type;
};

int txn_begin(TXN *parent,
              LOGGER *logger,
              TXN_ISOLATION_TYPE iso,
              int readonly,
              TXN **txn);
int txn_commit(TXN *txn);
int txn_abort(TXN *txn);
void txn_finish(TXN *txn);

#endif /* nessDB_TXN_H_ */
