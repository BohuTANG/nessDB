/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_FLUSHER_H_
#define nessDB_FLUSHER_H_

struct flusher_extra {
	struct buftree *tree;
	struct node *node;
	struct nmb *buffer;
};

void flush_some_child(struct buftree *t, struct node *parent);
void buftree_flush_node_on_background(struct buftree *t, struct node *parent);

#endif /* nessDB_FLUSHER_H_ */
