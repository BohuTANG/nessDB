/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "c.h"
#include "t.h"

void leaf_new(struct hdr *hdr,
              NID nid,
              uint32_t height,
              uint32_t children,
              struct node **n)
{
	struct node *node;

	nassert(height == 0);
	nassert(children == 1);
	node = xcalloc(1, sizeof(*node));
	nassert(node);
	node->nid = nid;
	node->height = height;
	mutex_init(&node->attr.mtx);
	node->n_children = children;
	node->layout_version = hdr->layout_version;

	if (children > 0) {
		node->pivots = xcalloc(children - 1, PIVOT_SIZE);
		node->parts = xcalloc(children, PART_SIZE);
	}

	node->opts = hdr->opts;
	node->i = &leaf_operations;
	node_set_dirty(node);

	*n = node;
}

void leaf_free(struct node *leaf)
{
	int i;
	nassert(leaf != NULL);
	nassert(leaf->height == 0);

	for (i = 0; i < (leaf->n_children - 1); i++)
		xfree(leaf->pivots[i].data);

	for (i = 0; i < leaf->n_children; i++)
		lmb_free(leaf->parts[i].msgbuf);
	mutex_destroy(&leaf->attr.mtx);
	xfree(leaf->pivots);
	xfree(leaf->parts);
	xfree(leaf);
}

void leaf_msgbuf_init(struct node *leaf)
{
	leaf->parts[0].msgbuf = lmb_new(leaf->opts);
}

/*
 * apply a msg to leaf msgbuf
 * neither the leaf type is LE_CLEAN or LE_MVCC
 * msgbuf will always keep multi-snapshot
 * the difference between LE_CLEAN and LE_MVCC is gc affects.
 *
 */
int leaf_put(struct node *leaf, struct bt_cmd *cmd)
{
	int ret = NESS_ERR;
	struct lmb *buffer = (struct lmb*)leaf->parts[0].msgbuf;

	switch (cmd->type & 0xff) {
	case MSG_INSERT:
		lmb_put(buffer,
		        cmd->msn,
		        cmd->type,
		        cmd->key,
		        cmd->val,
		        &cmd->xidpair);
		return NESS_OK;
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

	leaf->msn = cmd->msn > leaf->msn ? cmd->msn : leaf->msn;
	node_set_dirty(leaf);

	return ret;
}

/*
 * EFFECT:
 *	- split leaf&lmb into two leaves:a & b
 *	  a&b are both the half of the lmb
 *
 * PROCESS:
 *	- leaf:
 *		+-----------------------------------+
 *		|  0  |  1  |  2  |  3  |  4  |  5  |
 *		+-----------------------------------+
 *
 *	- split:
 *				   root
 *				 +--------+
 *				 |   2    |
 *				 +--------+
 *              /          \
 *	+-----------------+	 +------------------+
 *	    	|  0  |  1  |  2  |	 |  3  |  4  |  5   |
 *	    	+-----------------+	 +------------------+
 *	    	      nodea			nodeb
 *
 * ENTER:
 *	- leaf is already locked (L_WRITE)
 * EXITS:
 *	- a is locked
 *	- b is locked
 */
void leaf_split(void *tree,
                struct node *node,
                struct node **a,
                struct node **b,
                struct msg **split_key)
{
	struct partition *pa;
	struct partition *pb;
	struct node *leafa;
	struct node *leafb;
	struct lmb *mb;
	struct lmb *mba;
	struct lmb *mbb;
	struct msg *sp_key = NULL;
	struct buftree *t = (struct buftree*)tree;

	leafa = node;
	pa = &leafa->parts[0];

	/* split lmb of leaf to mba & mbb */
	mb = pa->msgbuf;
	lmb_split(mb, &mba, &mbb, &sp_key);
	lmb_free(mb);

	/* reset leafa buffer */
	pa->msgbuf = mba;

	/* new leafb */
	NID nid = hdr_next_nid(t->hdr);
	leaf_new(t->hdr,
	         nid,
	         0,
	         1,
	         &leafb);
	leaf_msgbuf_init(leafb);

	cache_put_and_pin(t->cf, nid, leafb);

	pb = &leafb->parts[0];
	lmb_free(pb->msgbuf);
	pb->msgbuf = mbb;

	/* set dirty */
	node_set_dirty(leafa);
	node_set_dirty(leafb);

	*a = leafa;
	*b = leafb;
	*split_key = sp_key;
}

/*
 * apply parent's (left, right] messages to child node
 */
void leaf_apply(struct node *leaf,
                struct nmb *msgbuf,
                struct msg *left,
                struct msg *right)
{
	struct mb_iter iter;
	struct pma_coord coord_left;
	struct pma_coord coord_right;

	nmb_get_left_coord(msgbuf, left, &coord_left);
	nmb_get_right_coord(msgbuf, right, &coord_right);

	mb_iter_init(&iter, msgbuf->pma);
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

		if (cmd.msn > leaf->msn) {
			leaf_put(leaf, &cmd);
		}
	}
}

int leaf_find_heaviest_idx(struct node *leaf)
{
	int i;
	int idx = 0;
	uint32_t sz = 0;
	uint32_t maxsz = 0;

	for (i = 0; i < leaf->n_children; i++) {
		struct lmb *msgbuf = (struct lmb*)&leaf->parts[i].msgbuf;

		sz = lmb_memsize(msgbuf);
		if (sz > maxsz) {
			idx = i;
			maxsz = sz;
		}
	}

	return idx;
}

uint32_t leaf_size(struct node *leaf)
{
	int i;
	uint32_t sz = 0U;

	for (i = 0; i < leaf->n_children; i++) {
		struct lmb *lmb = leaf->parts[0].msgbuf;

		sz += lmb_memsize(lmb);
	}

	return sz;
}

uint32_t leaf_count(struct node *leaf)
{
	int i;
	uint32_t c = 0U;

	for (i = 0; i < leaf->n_children; i++) {
		struct lmb *lmb = leaf->parts[0].msgbuf;

		c += lmb_count(lmb);
	}

	return c;
}

struct node_operations leaf_operations = {
	.put			= &leaf_put,
	.apply			= &leaf_apply,
	.split			= &leaf_split,
	.free			= &leaf_free,
	.size			= &leaf_size,
	.count			= &leaf_count,
	.mb_packer		= &lmb_to_msgpack,
	.mb_unpacker	= &msgpack_to_lmb,
	.init_msgbuf	= &leaf_msgbuf_init,
	.find_heaviest	= &leaf_find_heaviest_idx,
};
