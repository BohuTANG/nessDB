/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"

struct tree_operations rolltree_operations = {
	.fetch_node = NULL,
	.flush_node = NULL,
	.fetch_hdr = NULL,
	.flush_hdr = NULL,
	.free_node = NULL,
	.cache_put = NULL,
	.node_is_root = NULL,
	.node_is_dirty = NULL,
	.node_set_nondirty = NULL,
};
