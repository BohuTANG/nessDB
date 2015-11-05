/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TXNMGR_H_
#define nessDB_TXNMGR_H_

struct txnid_snapshot {
	int size;
	int used;
	TXNID *txnids;
};

struct txnmgr {
	TXNID last_txnid;
	ness_mutex_t mtx;
	struct txnid_snapshot *live_root_txnids;
};

struct txnmgr *txnmgr_new(void);
void txnmgr_txn_start(struct txnmgr* tm, TXN *txn);
void txnmgr_child_txn_start(struct txnmgr* tm, TXN *parnet, TXN *txn);
int txnmgr_txn_islive(struct txnid_snapshot *lives, TXNID txnid);
void txnmgr_live_root_txnid_del(struct txnmgr *tm, TXNID txnid);
void txnmgr_free(struct txnmgr *tm);

#endif /* nessDB_TXNMGR_H_*/
