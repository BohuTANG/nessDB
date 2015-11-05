/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "t.h"

int buftree_fetch_node_callback(int fd, void *hdr, NID nid, void **n)
{
	int r;
	struct timespec t1, t2;
	struct hdr *h = (struct hdr*)hdr;

	ngettime(&t1);
	r = deserialize_node_from_disk(fd, h, nid, (struct node**)n);
	ngettime(&t2);
	atomic64_add(&h->e->status->tree_node_fetch_costs, (uint64_t)time_diff_ms(t1, t2));
	atomic64_increment(&h->e->status->tree_node_fetch_nums);

	if (r != NESS_OK)
		__PANIC("fetch node from disk error, errno [%d]", r);

	return r;
}

int buftree_flush_node_callback(int fd, void *hdr, void *n)
{
	int r;
	struct timespec t1, t2;
	struct hdr *h = (struct hdr*)hdr;

	ngettime(&t1);
	r = serialize_node_to_disk(fd, n, h);
	ngettime(&t2);
	h->e->status->tree_node_flush_costs += time_diff_ms(t1, t2);
	h->e->status->tree_node_flush_nums++;
	if (r != NESS_OK)
		__PANIC("flush node to disk error, errno [%d]", r);

	return r;
}

int buftree_fetch_hdr_callback(int fd, void *hdr)
{
	int r;

	r = deserialize_hdr_from_disk(fd, hdr);
	if (r != NESS_OK)
		__PANIC("fetch tree header from disk error, errno [%d]", r);

	return r;
}

int buftree_flush_hdr_callback(int fd, void *hdr)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = serialize_hdr_to_disk(fd, h);
	if (r != NESS_OK)
		__PANIC("flush tree header to disk error, errno [%d]", r);

	return r;
}

int buftree_free_node_callback(void *n)
{
	node_free(n);
	return NESS_OK;
}

int buftree_cache_put_callback(void *n, void *cpair)
{
	struct node *node = (struct node*)n;

	node->cpair = cpair;
	return NESS_OK;
}

int buftree_node_is_dirty_callback(void *n)
{
	struct node *node = (struct node*)n;

	return node_is_dirty(node);
}

int buftree_node_set_nondirty_callback(void *n)
{
	struct node *node = (struct node*)n;

	node_set_nondirty(node);
	return NESS_OK;
}

struct tree_callback buftree_cb = {
	.fetch_node_cb = buftree_fetch_node_callback,
	.flush_node_cb = buftree_flush_node_callback,
	.fetch_hdr_cb = buftree_fetch_hdr_callback,
	.flush_hdr_cb = buftree_flush_hdr_callback,
	.free_node_cb = buftree_free_node_callback,
	.cache_put_cb = buftree_cache_put_callback,
	.node_is_dirty_cb = buftree_node_is_dirty_callback,
	.node_set_nondirty_cb = buftree_node_set_nondirty_callback,
};
