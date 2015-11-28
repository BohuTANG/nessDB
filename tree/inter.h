/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_INTER_H_
#define nessDB_INTER_H_
void inter_new(struct hdr *hdr,
               NID nid,
               uint32_t height,
               uint32_t children,
               struct node **n);
struct node_operations inter_operations;
#endif /* nessDB_INTER_H_ */
