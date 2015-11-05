/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TREE_H_
#define nessDB_TREE_H_

/**
 *
 * @file tree.h
 * @brief a buffered-tree index data structure
 * http://cs.au.dk/~large/Paperpages/bufferalgo.htm
 *
 */

#include "cache.h"
struct tree {
	int fd;
	struct hdr *hdr;
	struct cache_file *cf;
	struct env *e;
};

struct cache;
struct tree *tree_open(const char *dbname,
                       struct env *e,
                       struct tree_callback *tcb);

int tree_put(struct tree *t,
             struct msg *k,
             struct msg *v,
             msgtype_t type,
             TXN *txn);
void tree_free(struct tree *t);

int root_put_cmd(struct tree *t, struct bt_cmd *cmd);
void leaf_put_cmd(struct node *node, struct bt_cmd *cmd);
void nonleaf_put_cmd(struct node *node, struct bt_cmd *cmd);
void node_put_cmd(struct tree *t, struct node *node, struct bt_cmd *cmd);
enum reactivity get_reactivity(struct tree *t, struct node *node);
void node_split_child(struct tree *t, struct node *parent, struct node *child);

struct cpair_attr make_cpair_attr(struct node *n);
#endif /* nessDB_TREE_H_ */
