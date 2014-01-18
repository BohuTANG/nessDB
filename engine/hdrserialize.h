/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_HDRSERIALIZE_H_
#define nessDB_HDRSERIALIZE_H_

#include "internal.h"
#include "block.h"
#include "tree.h"
#include "node.h"
#include "file.h"

/**
 * @file hdrserialize.h
 * @brief
 *	layout of the buffered-tree header
 *	serializer and deserializer
 *
 */

int serialize_hdr_to_disk(int fd, struct block *b, struct hdr *hdr);
int deserialize_hdr_from_disk(int fd, struct block *b, struct hdr **hdr);

#endif /* nessDB_HDRSERIALIZE_H_ */
