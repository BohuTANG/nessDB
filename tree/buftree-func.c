/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"

int buftree_fetch_node(int fd, void *hdr, NID nid, void **n)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = deserialize_node_from_disk(fd, h, nid, (struct node**)n);
	if (r != NESS_OK)
		__PANIC("fetch node from disk error, errno [%d]", r);

	return r;
}

int buftree_flush_node(int fd, void *hdr, void *n)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = serialize_node_to_disk(fd, n, h);
	if (r != NESS_OK)
		__PANIC("flush node to disk error, errno [%d]", r);

	return r;
}

int buftree_fetch_hdr(int fd, void *hdr)
{
	int r;

	r = deserialize_hdr_from_disk(fd, hdr);
	if (r != NESS_OK)
		__PANIC("fetch tree header from disk error, errno [%d]", r);

	return r;
}

int buftree_flush_hdr(int fd, void *hdr)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = serialize_hdr_to_disk(fd, h);
	if (r != NESS_OK)
		__PANIC("flush tree header to disk error, errno [%d]", r);

	return r;
}

int buftree_free_node(void *n)
{
	struct node *node = (struct node*)n;

	node->i->free(n);

	return NESS_OK;
}

int buftree_cache_put(void *n, void *cpair)
{
	struct node *node = (struct node*)n;

	node->cpair = cpair;
	return NESS_OK;
}

int buftree_node_is_dirty(void *n)
{
	struct node *node = (struct node*)n;

	return node_is_dirty(node);
}

int buftree_node_set_nondirty(void *n)
{
	struct node *node = (struct node*)n;

	node_set_nondirty(node);
	return NESS_OK;
}

uint64_t buftree_node_size(void *n)
{
	struct node *node = (struct node*)n;

	return node->i->size(node);
}

struct tree_operations buftree_operations = {
	.fetch_hdr			= &buftree_fetch_hdr,
	.flush_hdr			= &buftree_flush_hdr,
	.fetch_node			= &buftree_fetch_node,
	.flush_node			= &buftree_flush_node,
	.free_node			= &buftree_free_node,
	.cache_put			= &buftree_cache_put,
	.node_size			= &buftree_node_size,
	.node_is_dirty		= &buftree_node_is_dirty,
	.node_set_nondirty	= &buftree_node_set_nondirty,
};
