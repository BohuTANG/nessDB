/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_HDR_H_
#define nessDB_HDR_H_

#include "internal.h"
#include "block.h"

/**
 * @file hdr.h
 * @brief
 *	layout of the buffered-tree header
 *	serializer and deserializer
 *
 */

struct hdr {
	int dirty;
	int height;
	int version;
	uint32_t blocksize;
	uint64_t lastseq;

	NID last_nid;
	NID root_nid;
	MSN last_msn;
	DISKOFF blockoff;
	struct options *opts;
};

int serialize_hdr_to_disk(int fd, struct block *b, struct hdr *hdr);
int deserialize_hdr_from_disk(int fd, struct block *b, struct hdr **hdr);

#endif /* nessDB_HDR_H_ */
