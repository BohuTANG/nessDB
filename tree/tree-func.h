/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TREE_FUNC_H_
#define nessDB_TREE_FUNC_H_

#include "internal.h"
#include "node.h"

int tree_fetch_node_callback(int fd, void *hdr, NID nid, void **n);
int tree_flush_node_callback(int fd, void *hdr, void *n);

int tree_fetch_hdr_callback(int fd, void *hdr);
int tree_flush_hdr_callback(int fd, void *hdr);

int tree_cache_put_callback(void *n, void *cpair);
int tree_free_node_callback(void *n);

int tree_node_is_dirty_callback(void *n);
int tree_node_set_nondirty_callback(void *n);

#endif /* nessDB_TREE_FUNC_H_ */
