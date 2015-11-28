/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_BUFTREE_H_
#define nessDB_BUFTREE_H_

/**
 *
 * @file buftree.h
 * @brief a buffered-tree index data structure
 * http://cs.au.dk/~large/Paperpages/bufferalgo.htm
 *
 */

enum {BT_DEFAULT_ROOT_FANOUT = 16};
enum {BT_DEFAULT_ROOT_NODE_SIZE = 4 * 1024 * 1024};

enum {BT_DEFAULT_INTER_FANOUT = 16};
enum {BT_DEFAULT_INTER_NODE_SIZE = 4 * 1024 * 1024};

enum {BT_DEFAULT_LEAF_NODE_SIZE = 4 * 1024 * 1024};
enum {BT_DEFAULT_LEAF_BASEMENT_SIZE = 512 * 1024};

struct buftree {
	int fd;
	struct hdr *hdr;
	struct cache_file *cf;
};

struct buftree *buftree_open(const char *dbname, struct cache *cache);
void buftree_free(struct buftree *t);

int buftree_put(struct buftree *t,
                struct msg *k,
                struct msg *v,
                msgtype_t type,
                TXN *txn);

int root_put_cmd(struct buftree *t, struct bt_cmd *cmd);
void node_put_cmd(struct buftree *t, struct node *node, struct bt_cmd *cmd);
void leaf_put_cmd(struct node *node, struct bt_cmd *cmd);
void nonleaf_put_cmd(struct node *node, struct bt_cmd *cmd);

void node_split_child(struct buftree *t, struct node *parent, struct node *child);

void buftree_set_node_fanout(struct hdr *hdr, int fanout);
void buftree_set_compress_method(struct hdr *hdr, int compress_method);

#endif /* nessDB_TREE_H_ */
