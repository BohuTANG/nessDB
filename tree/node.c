/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "node.h"

static struct node_operations nop = {
	.update_func = NULL,
	.delete_all_func = NULL,
	.pivot_compare_func = msg_key_compare
};

struct node *leaf_alloc_empty(NID nid) {
	struct node *node;

	node = xcalloc(1, sizeof(*node));
	node->nid = nid;
	node->height = 0;
	node->node_op = &nop;

	mutex_init(&node->u.l.mtx);
	mutex_init(&node->attr.mtx);
	ness_rwlock_init(&node->u.l.rwlock);

	return node;
}

void leaf_alloc_buffer(struct node *node)
{
	nassert(node->height == 0);
	node->u.l.buffer = lmb_new();
}

struct node *nonleaf_alloc_empty(NID nid, uint32_t height, uint32_t children) {
	int i;
	struct node *node;

	nassert(height > 0);
	nassert(children > 0);

	node = xcalloc(1, sizeof(*node));
	node->nid = nid;
	node->height = height;
	node->node_op = &nop;
	node->u.n.n_children = children;

	node->u.n.pivots = xcalloc(children - 1, PIVOT_SIZE);
	node->u.n.parts = xcalloc(children, PART_SIZE);

	for (i = 0; i < (int)children; i++) {
		mutex_init(&node->u.n.parts[i].mtx);
		ness_rwlock_init(&node->u.n.parts[i].rwlock);
	}
	mutex_init(&node->attr.mtx);

	return node;
}

void nonleaf_alloc_buffer(struct node *node)
{
	int i;

	nassert(node->height > 0);
	nassert(node->u.n.n_children > 0);
	for (i = 0; i < (int)node->u.n.n_children; i++) {
		node->u.n.parts[i].buffer = nmb_new();
	}
}

void node_set_dirty(struct node *node)
{
	mutex_lock(&node->attr.mtx);
	if (!node->dirty)
		gettime(&node->modified);
	node->dirty = 1;
	mutex_unlock(&node->attr.mtx);
}

void node_set_nondirty(struct node *node)
{
	mutex_lock(&node->attr.mtx);
	node->dirty = 0;
	mutex_unlock(&node->attr.mtx);
}

int node_is_dirty(struct node *node)
{
	int ret;

	mutex_lock(&node->attr.mtx);
	ret = (int)(node->dirty == 1);
	mutex_unlock(&node->attr.mtx);

	return ret;
}

/*
 * the rule is k <= pivot
 *
 *   i0   i1   i2   i3
 * +----+----+----+----+
 * | 15 | 17 | 19 | +∞ |
 * +----+----+----+----+
 *
 * so if key is 16, we will get 1(i1)
 */
int node_partition_idx(struct node *node, struct msg *k)
{
	int lo;
	int hi;
	int mi;
	int cmp;

	nassert(node->u.n.n_children > 1);
	lo = 0;
	hi = node->u.n.n_children - 2;
	while (lo <= hi) {
		/* mi integer overflow never happens */
		mi = (lo + hi) / 2;
		cmp = node->node_op->pivot_compare_func(k, &node->u.n.pivots[mi]);
		if (cmp > 0)
			lo = mi + 1;
		else if (cmp < 0)
			hi = mi - 1;
		else {
			return mi;
		}
	}

	return lo;
}

int node_find_heaviest_idx(struct node *node)
{
	int i;
	int idx = 0;
	uint32_t maxsz = 0;

	for (i = 0; i < (int)node->u.n.n_children; i++) {
		uint32_t sz;
		struct partition *part;

		part = &node->u.n.parts[i];
		sz = nmb_memsize(part->buffer);
		if (sz > maxsz) {
			idx = i;
			maxsz = sz;
		}
	}

	return idx;
}

uint32_t node_count(struct node *n)
{
	uint32_t c = 0U;

	if (n->height == 0)
		c += lmb_count(n->u.l.buffer);
	else {
		uint32_t i;
		for (i = 0; i < n->u.n.n_children; i++) {
			c += n->u.n.parts[i].buffer->count;
		}
	}

	return c;
}

uint32_t node_size(struct node *n)
{
	uint32_t size = 0U;

	size += (sizeof(*n));
	if (nessunlikely(n->height == 0)) {
		size += lmb_memsize(n->u.l.buffer);
	} else {
		uint32_t i;

		for (i = 0; i < n->u.n.n_children - 1; i++) {
			size += msgsize(&n->u.n.pivots[i]);
		}

		for (i = 0; i < n->u.n.n_children; i++) {
			size += nmb_memsize(n->u.n.parts[i].buffer);
		}
	}

	return size;
}

void node_free(struct node *node)
{
	nassert(node != NULL);

	if (node->height == 0) {
		lmb_free(node->u.l.buffer);
	} else {
		uint32_t i;

		if (node->u.n.n_children > 0) {
			for (i = 0; i < node->u.n.n_children - 1; i++) {
				xfree(node->u.n.pivots[i].data);
			}

			for (i = 0; i < node->u.n.n_children; i++) {
				nmb_free(node->u.n.parts[i].buffer);
			}

			xfree(node->u.n.pivots);
			xfree(node->u.n.parts);
		}
	}

	xfree(node);
}
