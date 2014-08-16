/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "layout.h"
#include "tree-func.h"

int tree_fetch_node_callback(int fd, void *hdr, NID nid, void **n)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = deserialize_node_from_disk(fd, h, nid, (struct node**)n);
	if (r != NESS_OK)
		__PANIC("fetch node from disk error, errno [%d]", r);

	return r;
}

int tree_flush_node_callback(int fd, void *hdr, void *n)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = serialize_node_to_disk(fd, n, h);
	if (r != NESS_OK)
		__PANIC("flush node to disk error, errno [%d]", r);

	return r;
}

int tree_fetch_hdr_callback(int fd, void *hdr)
{
	int r;

	r = deserialize_hdr_from_disk(fd, hdr);
	if (r != NESS_OK)
		__PANIC("fetch tree header from disk error, errno [%d]", r);

	return r;
}

int tree_flush_hdr_callback(int fd, void *hdr)
{
	int r;
	struct hdr *h = (struct hdr*)hdr;

	r = serialize_hdr_to_disk(fd, h);
	if (r != NESS_OK)
		__PANIC("flush tree header to disk error, errno [%d]", r);

	return r;
}

int tree_free_node_callback(void *n)
{
	node_free(n);
	return NESS_OK;
}

int tree_cache_put_callback(void *n, void *cpair)
{
	struct node *node = (struct node*)n;

	node->cpair = cpair;
	return NESS_OK;
}

int tree_node_is_dirty_callback(void *n)
{
	struct node *node = (struct node*)n;

	return node_is_dirty(node);
}

int tree_node_set_nondirty_callback(void *n)
{
	struct node *node = (struct node*)n;

	node_set_nondirty(node);
	return NESS_OK;
}
