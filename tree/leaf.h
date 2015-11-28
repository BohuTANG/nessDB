/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LEAF_H_
#define nessDB_LEAF_H_

/**
 *
 * @file leaf.h
 * @brief leaf
 *
 */
void leaf_new(struct hdr *hdr,
              NID nid,
              uint32_t height,
              uint32_t children,
              struct node **n);

struct node_operations leaf_operations;

#endif /* nessDB_LEAF_H_ */
