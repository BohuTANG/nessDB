/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "tcursor.h"
#include "node.h"

/*
 * apply a msg to leaf basement
 * neither the leaf type is LE_CLEAN or LE_MVCC
 * basement will always keep multi-snapshot
 * the difference between LE_CLEAN and LE_MVCC is gc affects.
 *
 */
int leaf_apply_msg(struct node *leaf,
                   struct msg *k,
                   struct msg *v,
                   msgtype_t type,
                   MSN msn,
                   struct xids *xids)
{
	write_lock(&leaf->u.l.le->rwlock);
	switch (type & 0xff) {
	case MSG_INSERT:
	case MSG_DELETE:
	case MSG_UPDATE: {
			/* TODO: do msg update, node->node_op->update */
		}
	case MSG_COMMIT:
	case MSG_ABORT:
	default:
		basement_put(leaf->u.l.le->bsm, k, v, type, msn, xids);
	}
	leaf->msn = msn > leaf->msn ? msn : leaf->msn;
	node_set_dirty(leaf);
	write_unlock(&leaf->u.l.le->rwlock);

	return NESS_OK;
}

/*
 * TODO: (BohuTANG) to do gc on MVCC
 * a) if a commit txid(with the same key) is smaller than other, gc it
 */
int leaf_do_gc(struct node *leaf)
{
	(void)leaf;

	return NESS_OK;
}

/*
 * apply msgs from ances to leaf basement which are between left and right
 * REQUIRES:
 *  1) leaf write-lock
 *  2) ances all write-lock
 */
int leaf_apply_ancestor_msg(struct node *leaf, struct ancestors *ances)
{
	struct ancestors *ance;
	struct msg *left = NULL;
	struct msg *right = NULL;
	struct basement_iter iter;
	struct basement *bsm = leaf->u.l.le->bsm;

	basement_iter_init(&iter, bsm);
	basement_iter_seektofirst(&iter);
	if (basement_iter_valid(&iter))
		left = msgdup(&iter.key);

	basement_iter_seektolast(&iter);
	if (basement_iter_valid(&iter))
		right = msgdup(&iter.key);

	ance = ances;
	while (ance) {
		/* TODO: (BohuTANG) apply [leaf, right] to leaf */
		ance = ances->next;
	}

	msgfree(left);
	msgfree(right);

	return NESS_OK;
}
