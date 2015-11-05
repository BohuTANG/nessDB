/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "t.h"

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
		break;
	case MSG_UPDATE:
		break;
	case MSG_COMMIT:
		break;
	case MSG_ABORT:
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
 * apply parent's (left, right] messages to child node
 */
void _apply_msg_to_child(struct node *parent,
                         int child_num,
                         struct node *child,
                         struct msg *left,
                         struct msg *right)
{
	struct nmb *buffer;
	struct mb_iter iter;
	struct pma_coord coord_left;
	struct pma_coord coord_right;

	nassert(parent->height > 0);
	buffer = parent->parts[child_num].ptr.u.nonleaf->buffer;

	nmb_get_left_coord(buffer, left, &coord_left);
	nmb_get_right_coord(buffer, right, &coord_right);

	mb_iter_init(&iter, buffer->pma);
	while (mb_iter_on_range(&iter, &coord_left, &coord_right)) {
		struct nmb_values values;

		nmb_get_values(&iter, &values);

		struct bt_cmd cmd = {
			.msn = values.msn,
			.type = values.type,
			.key = &values.key,
			.val = &values.val,
			.xidpair = values.xidpair
		};

		if (cmd.msn > child->msn) {
			if (nessunlikely(child->height == 0))
				leaf_put_cmd(child, &cmd);
			else
				nonleaf_put_cmd(child, &cmd);
		}
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
	int childnum;
	struct node *node;
	struct msg *left, *right;
	struct ancestors *cur_ance;

	(void)leaf;
	nassert(ances);

	/* get the bounds (left, right] */
	cur_ance = ances;
	while (cur_ance && cur_ance->next)
		cur_ance = cur_ance->next;
	childnum = cur_ance->childnum;
	node = (struct node*)cur_ance->v;
	left = (childnum > 0) ? msgdup(&node->pivots[childnum - 1]) : NULL;
	right = (childnum == node->n_children) ? NULL : msgdup(&node->pivots[childnum]);

	cur_ance = ances;
	while (cur_ance && cur_ance->next) {
		_apply_msg_to_child(cur_ance->v,
		                    cur_ance->childnum,
		                    cur_ance->next->v,
		                    left,
		                    right);
		cur_ance = cur_ance->next;
	}

	msgfree(left);
	msgfree(right);

	return NESS_OK;
}
