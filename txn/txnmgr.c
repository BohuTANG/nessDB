/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "x.h"
#include "t.h"

struct txnmgr *txnmgr_new(void) {
	struct txnmgr *tm;

	tm = xcalloc(1, sizeof(*tm));
	tm->live_root_txnids = xcalloc(1, sizeof(struct txnid_snapshot));
	tm->live_root_txnids->size = 1024;
	tm->live_root_txnids->txnids = xmalloc(tm->live_root_txnids->size * sizeof(TXNID));
	mutex_init(&tm->mtx);

	return tm;
}

void _txnmgr_live_root_txnid_add(struct txnmgr *tm, TXNID txnid)
{
	struct txnid_snapshot *lives = tm->live_root_txnids;

	if (lives->size == lives->used) {
		lives->size = lives->size << 1;
		lives->txnids = xrealloc(lives->txnids,
		                         lives->size * sizeof(TXNID));
	}
	lives->txnids[lives->used++] = txnid;
}

void txnmgr_live_root_txnid_del(struct txnmgr *tm, TXNID txnid)
{
	int i;
	struct txnid_snapshot *lives = tm->live_root_txnids;

	for (i = 0; i < lives->used; i++) {
		TXNID tid = lives->txnids[i];
		if (tid == txnid) break;
	}

	xmemmove(&lives->txnids[i],
	         &lives->txnids[i + 1],
	         (lives->used - i - 1) * sizeof(TXNID));
	lives->used--;
}

int txnmgr_txn_islive(struct txnid_snapshot *lives, TXNID txnid)
{
	int i;
	for (i = 0; i < lives->used; i++)
		if (lives->txnids[i] == txnid)
			return 1;

	return 0;
}

void _txnid_snapshot_clone(struct txnid_snapshot *src,
                           struct txnid_snapshot **dst)
{
	struct txnid_snapshot *clone;

	if (src->used  == 0) {
		*dst = NULL;
		return;
	}

	clone = xcalloc(1, sizeof(*clone));
	clone->used = src->used;
	clone->size = src->used;
	clone->txnids = xcalloc(src->used, sizeof(TXNID));
	xmemcpy(clone->txnids, src->txnids, src->used * sizeof(TXNID));
	*dst = clone;
}

void _txnid_snapshot_free(struct txnid_snapshot *snapshot)
{
	xfree(snapshot->txnids);
	xfree(snapshot);
}

static inline int _txn_needs_snapshot(TXN *parent,
                                      TXN_SNAPSHOT_TYPE snapshot_type)
{
	/*
	 * we need a snapshot if the snapshot type is:
	 *	- a child
	 *	- root and we have no parent
	 *
	 * cases that we don't need a snapshot:
	 *	- when snapshot type is NONE
	 *	- when it is ROOT and we have a parent
	 */
	return ((snapshot_type != TXN_SNAPSHOT_NONE) &&
	        (parent == NULL || snapshot_type == TXN_SNAPSHOT_CHILD));
}


/*
 * REQUIRES:
 *	- txn has no parent
 */
void txnmgr_txn_start(struct txnmgr* tm, TXN *txn)
{
	int needs_snapshot;

	assert(txn->parent == NULL);
	mutex_lock(&tm->mtx);
	txn->txnid = tm->last_txnid++;
	txn->root_parent_txnid = txn->txnid;
	if (!txn->readonly)
		_txnmgr_live_root_txnid_add(tm, txn->txnid);

	needs_snapshot = _txn_needs_snapshot(txn->parent, txn->snapshot_type);
	if (needs_snapshot)
		_txnid_snapshot_clone(tm->live_root_txnids, &txn->txnid_clone);
	mutex_unlock(&tm->mtx);
}

void txnmgr_child_txn_start(struct txnmgr* tm, TXN *parent, TXN *child)
{
	int needs_snapshot;

	mutex_lock(&tm->mtx);
	child->txnid = tm->last_txnid++;
	child->root_parent_txnid = parent->root_parent_txnid;
	if (!child->readonly)
		_txnmgr_live_root_txnid_add(tm, child->txnid);

	needs_snapshot = _txn_needs_snapshot(parent, child->snapshot_type);
	if (needs_snapshot)
		_txnid_snapshot_clone(tm->live_root_txnids, &child->txnid_clone);
	else
		_txnid_snapshot_clone(parent->txnid_clone, &child->txnid_clone);
	parent->child = child;
	child->parent = parent;

	mutex_unlock(&tm->mtx);
}

void txnmgr_free(struct txnmgr *tm)
{
	_txnid_snapshot_free(tm->live_root_txnids);
	xfree(tm);
}
