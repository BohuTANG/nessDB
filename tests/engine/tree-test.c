/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "tree.h"
#include "tree-func.h"
#include "compare-func.h"
#include "cursor.h"
#include "cache.h"
#include "ctest.h"

#define KEY_SIZE (16)
#define VAL_SIZE (100)

char kbuf[KEY_SIZE];
char vbuf[VAL_SIZE];

static struct tree_callback tree_cb = {
	.fetch_node = fetch_node_callback,
	.flush_node = flush_node_callback,
	.fetch_hdr = fetch_hdr_callback,
	.flush_hdr = flush_hdr_callback
};

#define DBPATH0 "tree-test-0.brt"
CTEST(tree, open)
{
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	xreset();
	/* create */
	opts = options_new();
	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH0, opts, status, cache, &tree_cb);
	ASSERT_NOT_NULL(tree);


	/* free */
	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}

#define DBPATH1 "tree-test-1.brt"
/*
 * this test include:
 */
/*
CTEST(tree, split)
{
	int i;
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;

	xreset();
	// create
	opts = options_new();
	status = status_new();
	cache = cache_new(opts);
	tree = tree_open(DBPATH0, opts, status, cache, &tree_cb);
	ASSERT_NOT_NULL(tree);

	// set leaf max count
	opts->leaf_node_page_count = 5;
	opts->inner_node_page_count = 3;

	// insert 4 records
	for (i = 0; i < 5; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		tree_put(tree, &k, &v, MSG_INSERT, NULL);
	}
	// check 1
	ASSERT_EQUAL(NID_START + 1, tree->hdr->root_nid);
	ASSERT_EQUAL(NID_START + 1, tree->hdr->last_nid);
	ASSERT_EQUAL(5, tree->hdr->last_msn);
	ASSERT_EQUAL(0, tree->hdr->height);

	// leaf split
	// this kv will add to root msgbuf
	memset(kbuf, 0 , KEY_SIZE);
	memset(vbuf, 0 , VAL_SIZE);
	snprintf(kbuf, KEY_SIZE, "key-%d", i);
	snprintf(vbuf, VAL_SIZE, "val-%d", i);
	struct msg k = {.data = kbuf, .size = KEY_SIZE};
	struct msg v = {.data = vbuf, .size = VAL_SIZE};
	tree_put(tree, &k, &v, MSG_INSERT, NULL);

	// check 2
	ASSERT_EQUAL(NID_START + 1, tree->hdr->root_nid);
	ASSERT_EQUAL(NID_START + 1 + 1 + 1, tree->hdr->last_nid);
	ASSERT_EQUAL(6, tree->hdr->last_msn);
	ASSERT_EQUAL(1, tree->hdr->height);

	// root node
	int r;
	NID anid, bnid;
	struct node *root;
	r = cache_get_and_pin(tree->cf, NID_START + 1, &root, L_READ);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(1, root->height);
	ASSERT_EQUAL(0, fifo_count(root->u.n.parts[0].buffer));
	ASSERT_EQUAL(1, fifo_count(root->u.n.parts[1].buffer));
	ASSERT_EQUAL(1, root->isroot);
	ASSERT_EQUAL(NID_START + 1, root->nid);
	ASSERT_EQUAL(2, root->u.n.n_children);
	anid = root->u.n.parts[0].child_nid;
	ASSERT_EQUAL(5, anid);
	bnid = root->u.n.parts[1].child_nid;
	ASSERT_EQUAL(6, bnid);

	struct msg p0 = root->u.n.pivots[0];
	memset(kbuf, 0 , KEY_SIZE);
	snprintf(kbuf, KEY_SIZE, "key-%d", 2);
	struct msg e_p0 = {.data = kbuf, .size = KEY_SIZE};
	ASSERT_EQUAL(0, msg_key_compare(&p0, &e_p0));
	cache_unpin(tree->cf, root);


	struct msgbuf_iter itera;
	struct node *leafa;
	r = cache_get_and_pin(tree->cf, anid, &leafa, L_READ);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(5, leafa->msn);
	msgbuf_iter_init(&itera, leafa->u.l.buffer);
	msgbuf_iter_seektofirst(&itera);
	for (i = 0; i < 3; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		struct msg ik = {.data = kbuf, .size = KEY_SIZE};

		r = msgbuf_iter_valid(&itera);
		ASSERT_EQUAL(1, r);
		r = msg_key_compare(&ik, &itera.key);
		ASSERT_EQUAL(0, r);
		msgbuf_iter_next(&itera);
	}
	cache_unpin(tree->cf, leafa);

	struct msgbuf_iter iterb;
	struct node *leafb;
	r = cache_get_and_pin(tree->cf, bnid, &leafb, L_READ);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, leafb->msn);
	ASSERT_EQUAL(2, msgbuf_count(leafb->u.l.buffer));
	msgbuf_iter_init(&iterb, leafb->u.l.buffer);
	msgbuf_iter_seektofirst(&iterb);
	for (i = 3; i < 5; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		struct msg ik = {.data = kbuf, .size = KEY_SIZE};

		r = msgbuf_iter_valid(&iterb);
		ASSERT_EQUAL(1, r);
		r = msg_key_compare(&ik, &iterb.key);
		ASSERT_EQUAL(0, r);
		msgbuf_iter_next(&iterb);
	}
	cache_unpin(tree->cf, leafb);

	// flush
	// now, in root msgbuf ,there is a key(key-5)
	for (i = 6; i < 9; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);
		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};
		tree_put(tree, &k, &v, MSG_INSERT, NULL);
	}
	r = cache_get_and_pin(tree->cf, NID_START + 1, &root, L_READ);
	ASSERT_EQUAL(1, r);
	ASSERT_EQUAL(0, fifo_count(root->u.n.parts[0].buffer));
	ASSERT_EQUAL(0, fifo_count(root->u.n.parts[1].buffer));
	ASSERT_EQUAL(1, fifo_count(root->u.n.parts[2].buffer));
	ASSERT_EQUAL(3, root->u.n.n_children);
	NID cnid = root->u.n.parts[2].child_nid;
	cache_unpin(tree->cf, root);

	struct msgbuf_iter iter;
	r = cache_get_and_pin(tree->cf, anid, &leafa, L_READ);
	ASSERT_EQUAL(3, msgbuf_count(leafa->u.l.buffer));
	msgbuf_iter_init(&iter, leafa->u.l.buffer);
	msgbuf_iter_seektofirst(&iter);
	for (i = 0; i < 3; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		struct msg ik = {.data = kbuf, .size = KEY_SIZE};

		r = msgbuf_iter_valid(&iter);
		ASSERT_EQUAL(1, r);
		r = msg_key_compare(&ik, &iter.key);
		ASSERT_EQUAL(0, r);
		msgbuf_iter_next(&iter);
	}
	cache_unpin(tree->cf, leafa);

	r = cache_get_and_pin(tree->cf, bnid, &leafb, L_READ);
	ASSERT_EQUAL(3, msgbuf_count(leafb->u.l.buffer));
	msgbuf_iter_init(&iter, leafb->u.l.buffer);
	msgbuf_iter_seektofirst(&iter);
	for (i = 3; i < 6; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		struct msg ik = {.data = kbuf, .size = KEY_SIZE};

		r = msgbuf_iter_valid(&iter);
		ASSERT_EQUAL(1, r);
		r = msg_key_compare(&ik, &iter.key);
		ASSERT_EQUAL(0, r);
		msgbuf_iter_next(&iter);
	}
	cache_unpin(tree->cf, leafb);

	struct node *leafc;
	r = cache_get_and_pin(tree->cf, cnid, &leafc, L_READ);
	ASSERT_EQUAL(2, msgbuf_count(leafc->u.l.buffer));
	msgbuf_iter_init(&iter, leafc->u.l.buffer);
	msgbuf_iter_seektofirst(&iter);
	for (i = 6; i < 8; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		struct msg ik = {.data = kbuf, .size = KEY_SIZE};
		r = msgbuf_iter_valid(&iter);
		ASSERT_EQUAL(1, r);
		r = msg_key_compare(&ik, &iter.key);
		ASSERT_EQUAL(0, r);
		msgbuf_iter_next(&iter);
	}
	cache_unpin(tree->cf, leafc);

	//free
	cache_free(cache);
	tree_free(tree);
	options_free(opts);
	status_free(status);
	xcheck_all_free();
}
*/
