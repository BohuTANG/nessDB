/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LEAF_H_
#define nessDB_LEAF_H_

#include "internal.h"
#include "node.h"
#include "msg.h"

/**
 *
 * @file leaf.h
 * @brief leaf
 *
 */

int leaf_apply_msg(struct node *leaf, struct bt_cmd *cmd);

#endif /* nessDB_LEAF_H_ */
