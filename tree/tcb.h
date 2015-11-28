/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TCB_H_
#define nessDB_TCB_H_

typedef uint64_t NID;
struct tree_operations {
	int (*fetch_node)(int fd, void *hdr, NID nid, void **n);
	int (*flush_node)(int fd, void *hdr, void *n);
	int (*free_node)(void *n);
	int (*fetch_hdr)(int fd, void *hdr);
	int (*flush_hdr)(int fd, void *hdr);
	int (*cache_put)(void *n, void *cpair);
	int (*node_is_dirty)(void *n);
	int (*node_set_nondirty)(void *n);
	uint64_t (*node_size)(void *n);
} NESSPACKED;

#endif /* nessDB_TREE_TCB_H_ */
