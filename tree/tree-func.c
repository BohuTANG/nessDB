/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "layout.h"
#include "debug.h"
#include "tree.h"
#include "tree-func.h"

int fetch_node_callback(void *tree, NID nid, struct node **n)
{
	int r;
	struct timespec t1, t2;
	struct tree *t = (struct tree*)tree;

	gettime(&t1);
	r = deserialize_node_from_disk(t->fd, t->block, nid, n, 0);
	gettime(&t2);

	atomic64_add(&t->status->tree_node_fetch_costs, (uint64_t)time_diff_ms(t1, t2));
	atomic64_increment(&t->status->tree_node_fetch_nums);
	if (r != NESS_OK)
		__PANIC("fetch node from disk error, errno [%d]", r);

	return r;
}

int flush_node_callback(void *tree, struct node *n)
{
	int r;
	struct timespec t1, t2;
	struct tree *t = (struct tree*)tree;

	gettime(&t1);
	r = serialize_node_to_disk(t->fd, t->block, n, t->hdr);
	gettime(&t2);
	t->status->tree_node_flush_costs += time_diff_ms(t1, t2);
	t->status->tree_node_flush_nums++;
	if (r != NESS_OK)
		__PANIC("flush node to disk error, errno [%d]", r);

	return r;
}

int fetch_hdr_callback(void *tree)
{
	int r;
	struct tree *t = (struct tree*)tree;

	r = deserialize_hdr_from_disk(t->fd, t->block, &t->hdr);
	if (r != NESS_OK)
		__PANIC("fetch tree header from disk error, errno [%d]", r);

	return r;
}

int flush_hdr_callback(void *tree)
{
	int r;
	struct tree *t = (struct tree*)tree;

	r = serialize_hdr_to_disk(t->fd, t->block, t->hdr);
	if (r != NESS_OK)
		__PANIC("flush tree header to disk error, errno [%d]", r);

	return r;
}
