/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "t.h"

struct tree_callback rolltree_cb = {
	.fetch_node_cb = NULL,
	.flush_node_cb = NULL,
	.fetch_hdr_cb = NULL,
	.flush_hdr_cb = NULL,
	.free_node_cb = NULL,
	.cache_put_cb = NULL,
	.node_is_dirty_cb = NULL,
	.node_set_nondirty_cb = NULL,
};
