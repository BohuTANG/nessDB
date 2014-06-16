/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_FLUSHER_H_
#define nessDB_FLUSHER_H_

#include "internal.h"
#include "block.h"
#include "node.h"
#include "mb.h"
#include "nmb.h"
#include "lmb.h"

struct flusher_extra {
	struct tree *tree;
	struct node *node;
	struct nmb *buffer;
};

void flush_some_child(struct tree *t, struct node *parent);
void tree_flush_node_on_background(struct tree *t, struct node *parent);

#endif /* nessDB_FLUSHER_H_ */
