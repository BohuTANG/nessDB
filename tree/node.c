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

struct leaf_basement_node *create_leaf(struct env *e) {
	struct leaf_basement_node *leaf = xcalloc(1, sizeof(*leaf));

	leaf->buffer = lmb_new(e);

	return leaf;
}

static void free_leaf(struct leaf_basement_node *leaf)
{
	lmb_free(leaf->buffer);
	xfree(leaf);
}

struct nonleaf_childinfo *create_nonleaf(struct env *e) {
	struct nonleaf_childinfo *nonleaf = xcalloc(1, sizeof(*nonleaf));

	nonleaf->buffer = nmb_new(e);

	return nonleaf;
}

static void free_nonleaf(struct nonleaf_childinfo *nonleaf)
{
	nmb_free(nonleaf->buffer);
	xfree(nonleaf);
}

void node_set_dirty(struct node *node)
{
	mutex_lock(&node->attr.mtx);
	if (!node->dirty)
		ngettime(&node->modified);
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
 * EFFECT:
 *	- alloc ptrs with nonleaf or leaf
 * REQUIRE:
 *	- node has height
 *	- node has n_children
 */
void node_ptrs_alloc(struct node *node)
{
	int i;

	for (i = 0; i < node->n_children; i++) {
		struct child_pointer *ptr = &node->parts[i].ptr;
		if (node->height > 0)
			ptr->u.nonleaf = create_nonleaf(node->env);
		else
			ptr->u.leaf = create_leaf(node->env);
	}
}

/*
 * EFFECT:
 *	- alloc node header
 */
struct node *node_alloc_empty(NID nid, int height, struct env *e) {
	struct node *node;

	node = xcalloc(1, sizeof(*node));
	node->nid = nid;
	node->height = height;
	node->node_op = &nop;
	node->env = e;

	mutex_init(&node->attr.mtx);
	node_set_dirty(node);

	return node;
}

/*
 * EFFECT:
 *	- just init and alloc pivots and parts
 */
void node_init_empty(struct node *node, int children, int layout_version)
{
	node->n_children = children;
	node->layout_version = layout_version;

	if (children > 0) {
		node->pivots = xcalloc(children - 1, PIVOT_SIZE);
		node->parts = xcalloc(children, PART_SIZE);
	}
}

/*
 * EFFECT:
 *	- alloc a full node: header + non/leaf infos
 */
struct node *node_alloc_full(NID nid,
                             int height,
                             int children,
                             int layout_version,
                             struct env *e) {
	struct node *node;

	node = node_alloc_empty(nid, height, e);
	node_init_empty(node, children, layout_version);
	node_ptrs_alloc(node);

	return node;
}

void node_free(struct node *node)
{
	int i;
	nassert(node != NULL);

	for (i = 0; i < (node->n_children - 1); i++)
		xfree(node->pivots[i].data);


	for (i = 0; i < node->n_children; i++) {
		struct child_pointer *ptr = &node->parts[i].ptr;

		if (node->height > 0)
			free_nonleaf(ptr->u.nonleaf);
		else
			free_leaf(ptr->u.leaf);
	}

	xfree(node->pivots);
	xfree(node->parts);
	xfree(node);
}

/*
 * PROCESS:
 *	- find the 1st pivot index which pivot >= k
 *	- it's lowerbound binary search
 *
 *	   i0   i1   i2   i3
 *	+----+----+----+----+
 *	| 15 | 17 | 19 | +∞ |
 *	+----+----+----+----+
 *	so if key is 16, we will get 1(i1)
 */
int node_partition_idx(struct node *node, struct msg *k)
{
	int lo = 0;
	int hi = node->n_children - 2;
	int best = hi;

	nassert(node->n_children > 1);
	while (lo <= hi) {
		int mid = (lo + hi) / 2;
		int cmp = node->node_op->pivot_compare_func(&node->pivots[mid], k);

		if (cmp >= 0) {
			best = mid;
			hi = mid - 1;
		} else {
			lo = mid + 1;
		}
	}

	return best;
}

int node_find_heaviest_idx(struct node *node)
{
	int i;
	int idx = 0;
	uint32_t sz = 0;
	uint32_t maxsz = 0;

	for (i = 0; i < node->n_children; i++) {
		struct child_pointer *ptr = &node->parts[i].ptr;

		if (node->height > 0)
			sz = nmb_memsize(ptr->u.nonleaf->buffer);
		else
			sz = lmb_memsize(ptr->u.leaf->buffer);

		if (sz > maxsz) {
			idx = i;
			maxsz = sz;
		}
	}

	return idx;
}

uint32_t node_count(struct node *node)
{
	int i;
	uint32_t c = 0U;

	for (i = 0; i < node->n_children; i++) {
		struct child_pointer *ptr = &node->parts[i].ptr;

		if (node->height > 0)
			c += nmb_count(ptr->u.nonleaf->buffer);
		else
			c += lmb_count(ptr->u.leaf->buffer);
	}

	return c;
}

uint32_t node_size(struct node *node)
{
	int i;
	uint32_t sz = 0;

	for (i = 0; i < node->n_children; i++) {
		struct child_pointer *ptr = &node->parts[i].ptr;

		if (node->height > 0)
			sz += nmb_memsize(ptr->u.nonleaf->buffer);
		else
			sz += lmb_memsize(ptr->u.leaf->buffer);
	}

	return sz;
}

static int _create_node(NID nid,
                        uint32_t height,
                        uint32_t children,
                        int version,
                        struct node **n,
                        struct env *e,
                        int light)
{
	struct node *new_node;

	if (light) {
		new_node = node_alloc_empty(nid, height, e);
		node_init_empty(new_node, children, version);
	} else {
		new_node = node_alloc_full(nid, height, children, version, e);
	}
	*n = new_node;

	return 1;
}

int node_create(NID nid,
                uint32_t height,
                uint32_t children,
                int version,
                struct env *e,
                struct node **n)
{
	return _create_node(nid, height, children, version, n, e, 0);
}

int node_create_light(NID nid,
                      uint32_t height,
                      uint32_t children,
                      int version,
                      struct env *e,
                      struct node **n)
{
	return _create_node(nid, height, children, version, n, e, 1);
}
