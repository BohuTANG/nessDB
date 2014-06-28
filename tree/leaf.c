/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "cursor.h"
#include "node.h"
#include "lmb.h"

int _le_apply_insert(struct lmb *mb, struct bt_cmd *cmd)
{
	lmb_put(mb,
	        cmd->msn,
	        cmd->type,
	        cmd->key,
	        cmd->val,
	        &cmd->xidpair);

	return NESS_OK;
}

/*
int _le_apply_delete(struct msgbuf *mb, struct bt_cmd *cmd)
{
	msgbuf_put(mb,
	           cmd->msn,
	           cmd->type,
	           cmd->key,
	           NULL,
	           &cmd->xidpair);

	return NESS_OK;
}

int _le_apply_commit(struct lmb *mb, struct bt_cmd *cmd)
{
	int cmp;
	int ret;
	struct mb_iter iter;

	lmb_iter_init(&iter, mb);

	mb_iter_seek(&iter, cmd->key);
	cmp = msg_key_compare(&iter.key, cmd->key);
	if (cmp == 0) {
		struct skipnode *node;

		node = iter.list_iter.node;
		if (node->num_px < 1) {
			ret = NESS_TXN_COMMIT_ERR;
			goto RET;
		}
		if (msgbuf_internal_iter_reverse(&iter)) {
			int same_pxid = iter.xidpair.parent_xid == cmd->xidpair.parent_xid;
			int same_xid = iter.xidpair.child_xid == cmd->xidpair.child_xid;
			if (same_pxid && same_xid) {
				nassert(node->used == (node->num_px + node->num_cx));
				node->num_cx++;
				node->num_px--;
				node->keys[node->num_cx] = node->keys[node->used - 1];
			}
		}
		ret = NESS_OK;
	} else {
		ret = NESS_TXN_COMMIT_ERR;
	}

	return ret;
}

int _le_apply_abort(struct msgbuf *mb, struct bt_cmd *cmd)
{
	int cmp;
	int ret;
	struct msgbuf_iter iter;

	msgbuf_iter_init(&iter, mb);

	msgbuf_iter_seek(&iter, cmd->key);
	cmp = msg_key_compare(&iter.key, cmd->key);
	if (cmp == 0) {
		struct skipnode *node;

		node = iter.list_iter.node;
		if (node->num_px < 1) {
			ret = NESS_TXN_COMMIT_ERR;
			goto RET;
		}
		if (msgbuf_internal_iter_reverse(&iter)) {
			int same_pxid = iter.xidpair.parent_xid == cmd->xidpair.parent_xid;
			int same_xid = iter.xidpair.child_xid == cmd->xidpair.child_xid;
			if (same_pxid && same_xid) {
				nassert(node->used == (node->num_px + node->num_cx));
				node->num_px--;
				node->used--;
			}
		}
		ret = NESS_OK;
	} else {
		ret = NESS_TXN_ABORT_ERR;
	}

	return ret;
}
*/

/*
 * apply a msg to leaf msgbuf
 * neither the leaf type is LE_CLEAN or LE_MVCC
 * msgbuf will always keep multi-snapshot
 * the difference between LE_CLEAN and LE_MVCC is gc affects.
 *
 */
int leaf_apply_msg(struct node *leaf, struct bt_cmd *cmd)
{
	int ret = NESS_ERR;
	struct lmb *buffer = leaf->parts[0].ptr.u.leaf->buffer;

	switch (cmd->type & 0xff) {
	case MSG_INSERT:
		ret = _le_apply_insert(buffer, cmd);
		break;
	case MSG_DELETE:
		//ret = _le_apply_delete(leaf->u.l.buffer, cmd);
		break;
	case MSG_UPDATE:
		break;
	case MSG_COMMIT:
		//ret = _le_apply_commit(leaf->u.l.buffer, cmd);
		break;
	case MSG_ABORT:
		//ret = _le_apply_abort(leaf->u.l.buffer, cmd);
		break;
	default:
		break;
	}

	return ret;
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
	(void)parent;
	(void)child_num;
	(void)child;
	(void)left;
	(void)right;
	/*
	int height;
	struct fifo *fifo;
	struct msgbuf_iter iter;

	nassert(child != NULL);
	nassert(parent->height > 0);

	height = child->height;
	fifo = parent->u.n.parts[child_num].buffer;

	fifo_iter_init(&iter, fifo);
	msgbuf_iter_seek(&iter, left);

	while (msgbuf_iter_valid_lessorequal(&iter, right)) {
		while (msgbuf_internal_iter_next(&iter)) {
			struct bt_cmd cmd = {
				.msn = iter.msn,
				.type = iter.type,
				.key = &iter.key,
				.val = &iter.val,
				.xidpair = iter.xidpair
			};

			if (cmd.msn > child->msn) {
				if (nessunlikely(height == 0))
					leaf_put_cmd(child, &cmd);
				else
					nonleaf_put_cmd(child, &cmd);
			}
		}
		msgbuf_iter_next(&iter);
	}
	*/
}

/*
 * apply msgs from ances to leaf msgbuf which are between(include) left and right
 * REQUIRES:
 *  1) leaf write-lock
 *  2) ances all write-lock
 */
/*
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
		//apply [leaf, right] to leaf
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
*/
