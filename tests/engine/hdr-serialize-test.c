/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */


#include "tree.h"
#include "node.h"
#include "block.h"
#include "hdrse.h"
#include "ctest.h"

#define BRT_FILE ("hdr-test.brt")

CTEST(hdr_serial_test, serial_deserial)
{
	int ret;
	int fd = ness_os_open(BRT_FILE, O_RDWR | O_CREAT, 0777);
	struct block *b = block_new();
	struct hdr *hdr = (struct hdr*)xcalloc(1, sizeof(*hdr));
	struct block_pair *pairs = xcalloc(3, sizeof(*pairs));

	hdr->root_nid = NID_START;
	block_init(b, pairs, 3);
	ret = serialize_hdr_to_disk(fd, b, hdr);
	xfree(hdr);
	ASSERT_EQUAL(1, ret);

	ret = deserialize_hdr_from_disk(fd, b, &hdr);
	xfree(hdr);
	ASSERT_EQUAL(1, ret);

	ness_os_close(fd);
	block_free(b);
	xfree(pairs);
	xcheck_all_free();
}
