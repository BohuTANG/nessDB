/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "l.h"
#include "x.h"

int txn_begin(TXN *parent,
              LOGGER *logger,
              TXN_ISOLATION_TYPE iso,
              int readonly,
              TXN **txn)
{
	struct txn *tx;
	struct txnmgr *txnmgr = logger->txnmgr;

	tx = xcalloc(1, sizeof(*tx));
	tx->readonly = readonly;
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
	/* if txn has parent, just copy rollback entry to parent */
	if (txn->parent) {
		TXN *parent = txn->parent;
		struct roll_entry *re = txn->rollentry;

		if (re)
			re->prev = parent->rollentry;

		while (re) {
			re->isref = 1;
			re = re->prev;
		}
		txn->parent = parent->child = NULL;
	}

	return NESS_OK;
}

int txn_abort(TXN *txn)
{
	(void)txn;
	/*
	int fn;
	struct tree *t = NULL;
	struct msg *rollkey = NULL;
	struct roll_entry *re = txn->rollentry;
	struct cache *c = txn->logger->cache;

	while (re) {
		switch (re->type) {
		case RT_CMDINSERT:
			fn = re->u.cmdinsert.filenum;
			t = cache_get_tree_by_filenum(c, fn);
			rollkey = re->u.cmdinsert.key;
			break;

		case RT_CMDDELETE:
			fn = re->u.cmddelete.filenum;
			t = cache_get_tree_by_filenum(c, fn);
			rollkey = re->u.cmddelete.key;
			break;
		case RT_CMDUPDATE:
			fn = re->u.cmdupdate.filenum;
			rollkey = re->u.cmdupdate.key;
			t = cache_get_tree_by_filenum(c, fn);
			break;
		}
		if (t) {
			struct bt_cmd cmd = {
				.type = MSG_ABORT,
				.msn = ZERO_MSN,
				.key = rollkey,
				.val = NULL
			};
			root_put_cmd(t, &cmd);
		}

		re = re->prev;
	}

	if (txn->parent) {
		TXN *parent = txn->parent;
		txn->parent = parent->child = NULL;
	}
	*/

	return NESS_OK;
}

void txn_finish(TXN *txn)
{
	struct roll_entry *prev;
	struct roll_entry *re = txn->rollentry;
	struct txnmgr *txnmgr = txn->logger->txnmgr;

	if (!txn->parent) {
		if (txn->rollentry) {

			/* make sure that all children are committed */
			nassert(txn->child == NULL);
			if (!txn->readonly)
				txnmgr_live_root_txnid_del(txnmgr, txn->txnid);

			while (re) {
				prev = re->prev;
				rollentry_free(re);
				re = prev;
			}
		}

	} else {
		if (txn->rollentry && txn->rollentry->isref == 0) {
			while (re) {
				prev = re->prev;
				rollentry_free(re);
				re = prev;
			}
		}
	}

	if (txn->txnid_clone) {
		xfree(txn->txnid_clone->txnids);
		xfree(txn->txnid_clone);
	}
	xfree(txn);
}
