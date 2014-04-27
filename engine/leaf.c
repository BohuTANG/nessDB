/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "tcursor.h"
#include "node.h"

/*
 * apply a msg to leaf msgbuf
 * neither the leaf type is LE_CLEAN or LE_MVCC
 * msgbuf will always keep multi-snapshot
 * the difference between LE_CLEAN and LE_MVCC is gc affects.
 *
 */
int leaf_apply_msg(struct node *leaf, struct bt_cmd *cmd)
{
	write_lock(&leaf->u.l.rwlock);
	switch (cmd->type & 0xff) {
	case MSG_INSERT:
	case MSG_DELETE:
	case MSG_UPDATE: {
			/* TODO: do msg update, node->node_op->update */
		}
	case MSG_COMMIT:
	case MSG_ABORT:
	default:
		msgbuf_put(leaf->u.l.buffer,
		           cmd->msn,
		           cmd->type,
		           cmd->key,
		           cmd->val,
		           &cmd->xidpair);
	}
	leaf->msn = cmd->msn > leaf->msn ? cmd->msn : leaf->msn;
	node_set_dirty(leaf);
	write_unlock(&leaf->u.l.rwlock);

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
 * apply parent's [leaf, right] messages to child node
 */
void _apply_msg_to_child(struct node *parent,
                         int child_num,
                         struct node *child,
                         struct msg *left,
                         struct msg *right)
{
	int height;
	struct msgbuf *mb;
	struct msgbuf_iter iter;

	nassert(child != NULL);
	nassert(parent->height > 0);

	height = child->height;
	if (height == 0)
		mb = child->u.l.buffer;
	else
		mb = child->u.n.parts[child_num].buffer;

	msgbuf_iter_init(&iter, mb);
	msgbuf_iter_seek(&iter, left);

	while (msgbuf_iter_valid_lessorequal(&iter, right)) {
		struct bt_cmd cmd = {
			.msn = iter.msn,
			.type = iter.type,
			.key = &iter.key,
			.val = &iter.val,
			.xidpair = iter.xidpair
		};

		if (nessunlikely(height == 0))
			leaf_put_cmd(child, &cmd);
		else
			nonleaf_put_cmd(child, &cmd);
	}
}

/*
 * apply msgs from ances to leaf msgbuf which are between(include) left and right
 * REQUIRES:
 *  1) leaf write-lock
 *  2) ances all write-lock
 */
int leaf_apply_ancestors(struct node *leaf, struct ancestors *ances)
{
	struct ancestors *ance;
	struct msg *left = NULL;
	struct msg *right = NULL;
	struct msgbuf_iter iter;
	struct msgbuf *mb = leaf->u.l.buffer;

	msgbuf_iter_init(&iter, mb);
	msgbuf_iter_seektofirst(&iter);
	if (msgbuf_iter_valid(&iter))
		left = msgdup(&iter.key);

	msgbuf_iter_seektolast(&iter);
	if (msgbuf_iter_valid(&iter))
		right = msgdup(&iter.key);

	ance = ances;
	while (ance && ance->next) {
		/* apply [leaf, right] to leaf */
		_apply_msg_to_child(ance->v,
		                    ance->childnum,
		                    ance->next->v,
		                    left,
		                    right);
		ance = ances->next;
	}

	msgfree(left);
	msgfree(right);

	return NESS_OK;
}
