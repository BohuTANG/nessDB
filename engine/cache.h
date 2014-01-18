/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CACHE_H_
#define nessDB_CACHE_H_

#include "internal.h"
#include "cpair.h"

/**
 * @file cache.h
 * @brief virtual cache(VCACHE)
 * if you want to implement a sub cache of vcache
 * you MUST implement the API defined in cache_operations
 */
struct cache_file {
	int fd;
	struct tree *tree;
};

struct cache;
struct cache_operations {
	/* get and pin a node from cache, SUCC: NESS_OK */
	int (*cache_get_and_pin) (struct cache *c, NID k, struct node **n, enum lock_type locktype);

	/* create and pin a node from cache, SUCC: NESS_OK */
	int (*cache_create_node_and_pin) (struct cache *c, uint32_t height, uint32_t children, struct node **n);

	/* unpin a dirty node, and update cache size */
	void (*cache_unpin) (struct cache *c, struct node *n);

	/* unpin a readonly node */
	void (*cache_unpin_readonly) (struct cache *c, struct node *n);

	/* swap the cache pair value and key */
	void (*cache_cpair_value_swap) (struct cache *c, struct node *a, struct node *b);

	void (*cache_file_register) (struct cache *c, struct cache_file *cfile);
	void (*cache_file_unregister) (struct cache *c, struct cache_file *cfile);
};

struct cache {
	void *extra;
	struct cache_operations *c_op;
};

#endif /* nessDB_CACHE_H_ */
