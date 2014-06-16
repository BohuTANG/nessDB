/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TREE_FUNC_H_
#define nessDB_TREE_FUNC_H_

#include "internal.h"
#include "tree.h"

int fetch_node_callback(void *tree, NID nid, struct node **n);
int flush_node_callback(void *tree, struct node *n);
int fetch_hdr_callback(void *tree);
int flush_hdr_callback(void *tree);

#endif /* nessDB_TREE_FUNC_H_ */
