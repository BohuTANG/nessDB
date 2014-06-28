/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LAYOUT_H_
#define nessDB_LAYOUT_H_

#include "internal.h"
#include "block.h"
#include "tree.h"
#include "node.h"
#include "file.h"

/**
 * @file layout.h
 * @brief serializer&deserializer of buffered-tree node
 *
 */
int serialize_node_to_disk(int fd,
                           struct block *b,
                           struct node *node,
                           struct hdr *hdr);

int deserialize_node_from_disk(int fd,
                               struct block *block,
                               struct hdr *hdr,
                               NID nid,
                               struct node **node);
#endif /* nessDB_LAYOUT_H_ */
