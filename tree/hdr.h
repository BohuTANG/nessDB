/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_HDR_H_
#define nessDB_HDR_H_

/**
 * @file hdr.h
 * @brief
 *	layout of the buffered-tree header
 *	serializer and deserializer
 *
 */

#define NID_START (3)
struct hdr {
	int dirty;
	int height;
	int layout_version;
	uint32_t blocksize;
	uint64_t lastseq;

	NID last_nid;
	NID root_nid;
	MSN last_msn;
	DISKOFF blockoff;
	struct block *block;
	struct tree_options *opts;
};

struct hdr *hdr_new(struct tree_options *opts);
void hdr_free(struct hdr *hdr);
NID hdr_next_nid(struct hdr *hdr);
MSN hdr_next_msn(struct hdr *hdr);
int serialize_hdr_to_disk(int fd, struct hdr *hdr);
int deserialize_hdr_from_disk(int fd, struct hdr *hdr);

#endif /* nessDB_HDR_H_ */
