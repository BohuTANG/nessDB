/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "c.h"
#include "t.h"

void inter_new(struct hdr *hdr,
               NID nid,
               uint32_t height,
               uint32_t children,
               struct node **n)
{
	struct node *node;

	nassert(height > 0);
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
	node->i = &inter_operations;
	node_set_dirty(node);

	*n = node;
}

void inter_free(struct node *node)
{
	int i;
	nassert(node != NULL);
	nassert(node->height > 0);

	for (i = 0; i < (node->n_children - 1); i++)
		xfree(node->pivots[i].data);

	for (i = 0; i < node->n_children; i++)
		nmb_free(node->parts[i].msgbuf);

	xfree(node->pivots);
	xfree(node->parts);
	mutex_destroy(&node->attr.mtx);
	xfree(node);
}

/*
 * EFFECT:
 *	- alloc ptrs with nonleaf or leaf
 * REQUIRE:
 *	- node has height
 *	- node has n_children
 */
void inter_msgbuf_init(struct node * node)
{
	int i;

	nassert(node->height > 0);
	for (i = 0; i < node->n_children; i++)
		node->parts[i].msgbuf = nmb_new(node->opts);
}

/*
 * EFFECT:
 *	- put cmd to nonleaf
 * ENTER:
 *	- node is already locked (L_READ)
 * EXITS:
 *	- node is locked
 */
int inter_put(struct node * node, struct bt_cmd * cmd)
{
	uint32_t pidx;
	struct nmb *buffer;

	pidx = node_partition_idx(node, cmd->key);
	buffer = node->parts[pidx].msgbuf;
	nassert(buffer);

	nmb_put(buffer,
	        cmd-> msn,
	        cmd->type,
	        cmd->key,
	        cmd->val,
	        &cmd->xidpair);
	node->msn = cmd->msn > node->msn ? cmd->msn : node->msn;
	node_set_dirty(node);

	return NESS_OK;
}

void inter_apply(struct node *node,
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

		if (cmd.msn > node->msn) {
			inter_put(node, &cmd);
		}
	}
}

void inter_split(void *tree,
                 struct node *node,
                 struct node **a,
                 struct node **b,
                 struct msg **split_key)
{
	int i;
	int pivots_old;
	int pivots_in_a;
	int pivots_in_b;
	struct node *nodea;
	struct node *nodeb;
	struct msg *spk;
	struct buftree *t = (struct buftree*)tree;

	nodea = node;
	pivots_old = node->n_children - 1;
	nassert(pivots_old > 2);

	pivots_in_a = pivots_old / 2;
	pivots_in_b = pivots_old - pivots_in_a;

	/* node a */
	nodea->n_children = pivots_in_a + 1;

	/* node b */
	NID nid = hdr_next_nid(t->hdr);
	inter_new(t->hdr,
	          nid,
	          node->height,
	          pivots_in_b + 1,
	          &nodeb);
	cache_put_and_pin(t->cf, nid, nodeb);

	for (i = 0; i < (pivots_in_b); i++)
		nodeb->pivots[i] = nodea->pivots[pivots_in_a + i];

	for (i = 0; i < (pivots_in_b + 1); i++)
		nodeb->parts[i] = nodea->parts[pivots_in_a + i];

	/* the rightest partition of nodea */
	struct partition *part = &nodea->parts[pivots_in_a];
	part->msgbuf = nmb_new(node->opts);

	/* split key */
	spk = msgdup(&node->pivots[pivots_in_a - 1]);

	node_set_dirty(nodea);
	node_set_dirty(nodeb);

	*a = nodea;
	*b = nodeb;
	*split_key = spk;
}

uint32_t inter_size(struct node * node)
{
	int i;
	uint32_t sz = 0;

	nassert(node->height > 0);
	for (i = 0; i < node->n_children; i++)
		sz += nmb_memsize(node->parts[i].msgbuf);

	return sz;
}

uint32_t inter_count(struct node * node)
{
	int i;
	uint32_t c = 0U;

	nassert(node->height > 0);
	for (i = 0; i < node->n_children; i++)
		c += nmb_count(node->parts[i].msgbuf);

	return c;
}

int inter_find_heaviest_idx(struct node * node)
{
	int i;
	int idx = 0;
	uint32_t sz = 0;
	uint32_t maxsz = 0;

	for (i = 0; i < node->n_children; i++) {
		sz = nmb_memsize(node->parts[i].msgbuf);
		if (sz > maxsz) {
			idx = i;
			maxsz = sz;
		}
	}

	return idx;
}

struct node_operations inter_operations = {
	.put			= &inter_put,
	.apply			= &inter_apply,
	.split			= &inter_split,
	.free			= &inter_free,
	.size			= &inter_size,
	.count			= &inter_count,
	.mb_packer		= &nmb_to_msgpack,
	.mb_unpacker	= &msgpack_to_nmb,
	.init_msgbuf	= &inter_msgbuf_init,
	.find_heaviest	= &inter_find_heaviest_idx,
};
