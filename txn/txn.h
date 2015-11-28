/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_TXN_H_
#define nessDB_TXN_H_

/**
 *
 * @file txn.h
 * @brief transaction
 *
 */

/* transaction state */
typedef enum txn_state {
	TX_LIVE,
	TX_PREPARING,
	TX_COMMITTING,
	TX_ABORTINT,
} TXNSTATE;

/* txn */
#define TXNID_NONE (0)
typedef enum {
	TXN_ISO_SERIALIZABLE = 0,
	TXN_ISO_REPEATABLE = 1,
	TXN_ISO_READ_COMMITTED = 2,
	TXN_ISO_READ_UNCOMMITTED = 3
} TXN_ISOLATION_TYPE;

/*
types of snapshots that can be taken by a txn
   - TXN_SNAPSHOT_NONE: means that there is no snapshot. Reads do not use snapshot reads.
			used for SERIALIZABLE and READ UNCOMMITTED
   - TXN_SNAPSHOT_ROOT: means that all txns use their root transaction's snapshot
			used for REPEATABLE READ
   - TXN_SNAPSHOT_CHILD: means that each child txn creates its own snapshot
			used for READ COMMITTED
*/
typedef enum {
	TXN_SNAPSHOT_NONE = 0,
	TXN_SNAPSHOT_ROOT = 1,
	TXN_SNAPSHOT_CHILD = 2
} TXN_SNAPSHOT_TYPE;


typedef struct txn {
	int readonly;
	TXNID txnid;
	TXNID root_parent_txnid;
	TXNSTATE state;
	LOGGER *logger;
	struct txn *parent;
	struct txn *child;
	struct roll_entry *rollentry;
	struct txnid_snapshot *txnid_clone;
	TXN_ISOLATION_TYPE iso_type;
	TXN_SNAPSHOT_TYPE snapshot_type;
} TXN;

int txn_begin(TXN *parent,
              LOGGER *logger,
              TXN_ISOLATION_TYPE iso,
              int readonly,
              TXN **txn);
int txn_commit(TXN *txn);
int txn_abort(TXN *txn);
void txn_finish(TXN *txn);

#endif /* nessDB_TXN_H_ */
