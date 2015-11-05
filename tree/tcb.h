/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TCB_H_
#define nessDB_TCB_H_

struct tree_callback {
	int (*fetch_node_cb)(int fd, void *hdr, NID nid, void **n);
	int (*flush_node_cb)(int fd, void *hdr, void *n);
	int (*free_node_cb)(void *n);
	int (*fetch_hdr_cb)(int fd, void *hdr);
	int (*flush_hdr_cb)(int fd, void *hdr);
	int (*cache_put_cb)(void *n, void *cpair);
	int (*node_is_dirty_cb)(void *n);
	int (*node_set_nondirty_cb)(void *n);
};

#endif /* nessDB_TREE_TCB_H_ */
