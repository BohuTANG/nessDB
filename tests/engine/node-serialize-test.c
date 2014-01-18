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
#include "serialize.h"
#include "ctest.h"

#define BRT_FILE ("node-test.brt")

CTEST(node_serial_test, leaf_empty) {
	int ret = 0;
	int fd = ness_os_open(BRT_FILE, O_RDWR|O_CREAT, 0777);
	struct block *b = block_new();
	struct hdr *hdr = (struct hdr*)xcalloc(1, sizeof(*hdr));
	struct options *opts = options_new();

	/*
	 * dummy brt leaf
	 */
	uint64_t nid = 3;

	struct node *dummy_leaf = leaf_alloc_empty(nid);
	leaf_alloc_bsm(dummy_leaf);
	ret = serialize_node_to_disk(fd, b, dummy_leaf, hdr);
	ASSERT_TRUE(ret > 0);
	//free
	node_free(dummy_leaf);

	struct node *dummy_leaf1;
	ret = deserialize_node_from_disk(fd, b, nid, &dummy_leaf1, 0);
	ASSERT_TRUE(ret > 0);
	ASSERT_EQUAL(0, basement_count(dummy_leaf1->u.l.le->bsm));
	//free
	node_free(dummy_leaf1);

	ness_os_close(fd);
	block_free(b);
	xfree(hdr);
	options_free(opts);
	xcheck_all_free();
}

CTEST(node_serial_test, leaf_2_record) {
	int ret = 0;
	int fd = ness_os_open(BRT_FILE, O_RDWR|O_CREAT, 0777);
	struct block *b = block_new();
	struct hdr *hdr = (struct hdr*)xcalloc(1, sizeof(*hdr));
	struct options *opts = options_new();

	/*
	 * dummy brt leaf
	 */
	uint64_t nid = 3;

	struct node *dummy_leaf = leaf_alloc_empty(nid);
	leaf_alloc_bsm(dummy_leaf);

	struct msg k, v;
	k.size = 6;
	k.data = "hello";
	v.size = 6;
	v.data = "world";
	basement_put(dummy_leaf->u.l.le->bsm, &k, &v, MSG_PUT);

	struct msg k1, v1;
	k1.size = 6;
	k1.data = "hellx";
	v1.size = 6;
	v1.data = "worlx";
	basement_put(dummy_leaf->u.l.le->bsm, &k1, &v1, MSG_PUT);

	ret = serialize_node_to_disk(fd, b, dummy_leaf, hdr);
	ASSERT_TRUE(ret > 0);

	//free
	node_free(dummy_leaf);

	struct node *dummy_leaf1;
	ret = deserialize_node_from_disk(fd, b, nid, &dummy_leaf1, 0);
	ASSERT_TRUE(ret > 0);

	ASSERT_EQUAL(2, basement_count(dummy_leaf1->u.l.le->bsm));

	struct basement_iter iter;
	basement_iter_init(&iter, dummy_leaf1->u.l.le->bsm);
	basement_iter_seek(&iter, &k);
	ASSERT_EQUAL(1, iter.valid);
	ASSERT_STR("world", iter.val.data);

	basement_iter_seek(&iter, &k1);
	ASSERT_EQUAL(1, iter.valid);
	ASSERT_STR("worlx", iter.val.data);

	//free
	node_free(dummy_leaf1);

	ness_os_close(fd);
	block_free(b);
	xfree(hdr);
	options_free(opts);
	xcheck_all_free();
}

CTEST(node_serial_test, node_2th_part_empty) {
	int ret = 0;
	NID nid;
	uint32_t n_children = 3;
	int fd = ness_os_open(BRT_FILE, O_RDWR|O_CREAT, 0777);
	struct block *b = block_new();
	struct hdr *hdr = (struct hdr*)xcalloc(1, sizeof(*hdr));
	struct options *opts = options_new();

	/*
	 * serialize
	 */
	struct node *dummy_node;

	hdr->last_nid++;
	nid = hdr->last_nid;
	dummy_node = nonleaf_alloc_empty(nid, 1, n_children);
	nonleaf_alloc_buffer(dummy_node);

	struct msg p0;
	p0.size = 6;
	p0.data = "pivot0";
	msgcpy(&dummy_node->u.n.pivots[0], &p0);

	struct msg p1;
	p1.size = 6;
	p1.data = "pivot1";
	msgcpy(&dummy_node->u.n.pivots[1], &p1);

	struct msg k, v;
	k.size = 5;
	k.data = "hello";
	v.size = 5;
	v.data = "world";
	basement_put(dummy_node->u.n.parts[0].buffer, &k, &v, MSG_PUT);

	
	hdr->method = NESS_QUICKLZ_METHOD;
	ret = serialize_node_to_disk(fd, b, dummy_node, hdr);
	ASSERT_TRUE(ret > 0);
	node_free(dummy_node);

	//deserialize
	int light = 0;
	struct node *dummy_node1;
	ret = deserialize_node_from_disk(fd, b, nid, &dummy_node1, light);
	ASSERT_TRUE(ret > 0);

	ASSERT_EQUAL(1, dummy_node1->height);
	ASSERT_EQUAL(3, dummy_node1->u.n.n_children);
	ASSERT_DATA((const unsigned char*)"pivot0",
			6,
			(const unsigned char*)dummy_node1->u.n.pivots[0].data,
			dummy_node1->u.n.pivots[0].size);

	ASSERT_DATA((const unsigned char*)"pivot1",
			6,
			(const unsigned char*)dummy_node1->u.n.pivots[1].data,
			dummy_node1->u.n.pivots[1].size);
	ASSERT_EQUAL(3, dummy_node1->u.n.n_children);

	if (!light) {
		int cmp;
		struct basement_iter iter;
		struct basement *bsm;

		bsm = dummy_node1->u.n.parts[0].buffer;
		basement_iter_init(&iter, bsm);

		int mb_c = basement_count(dummy_node1->u.n.parts[0].buffer);
		ASSERT_EQUAL(1, mb_c);
		basement_iter_seek(&iter, &k);
		ret = basement_iter_valid(&iter);
		ASSERT_EQUAL(1, ret);
		cmp = msgcmp(&k, &iter.key);
		ASSERT_EQUAL(0, cmp);
		cmp = msgcmp(&v, &iter.val);
		ASSERT_EQUAL(0, cmp);

		mb_c = basement_count(dummy_node1->u.n.parts[1].buffer);
		ASSERT_EQUAL(0, mb_c);
	}
	node_free(dummy_node1);

	ness_os_close(fd);
	block_free(b);
	xfree(hdr);
	options_free(opts);
	xcheck_all_free();
}
