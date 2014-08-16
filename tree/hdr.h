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
	struct block *block;
	struct env *e;
};

struct hdr *hdr_new(struct env *e);
void hdr_free(struct hdr *hdr);
NID hdr_next_nid(struct hdr *hdr);
MSN hdr_next_msn(struct hdr *hdr);
int serialize_hdr_to_disk(int fd, struct hdr *hdr);
int deserialize_hdr_from_disk(int fd, struct hdr *hdr);

#endif /* nessDB_HDR_H_ */
