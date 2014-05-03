/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "msgbuf.h"
#include "compare-func.h"
#include "debug.h"
#include "ctest.h"

CTEST(msgbuf, empty)
{
	int ret;
	struct msgbuf_iter iter;
	struct msgbuf *bsm = msgbuf_new();

	msgbuf_iter_init(&iter, bsm);
	msgbuf_iter_seektofirst(&iter);
	ret = msgbuf_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	struct msg m1 = {
		.size = 6,
		.data = "key-01"
	};

	msgbuf_iter_seek(&iter, &m1);
	ret = msgbuf_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	msgbuf_free(bsm);
	xcheck_all_free();
}

void _random_key(char *key, int length)
{
	int i;
	char salt[36] = "abcdefghijklmnopqrstuvwxyz123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

#define KEY_SIZE (16)
#define VAL_SIZE (100)
CTEST(msgbuf, insert_and_lookup)
{
	int i;
	int ret;
	int R = 1000;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct msgbuf_iter iter;
	struct msgbuf *bsm = msgbuf_new();
	struct msg **msgs = xcalloc(R, sizeof(*msgs));
	struct txnid_pair xidpair =  {
		.child_xid = TXNID_NONE,
		.parent_xid = TXNID_NONE
	};

	MSN msn = 0U;

	_random_key(vbuf, VAL_SIZE);
	for (i = 0; i < R; i++) {
		memset(kbuf, 0, KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		msgs[i] = msgdup(&k);
		msgbuf_put(bsm,
		           msn++,
		           MSG_INSERT,
		           &k,
		           &v,
		           &xidpair
		          );
	}

	msgbuf_iter_init(&iter, bsm);
	msgbuf_iter_seektolast(&iter);
	ret = msgbuf_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);

	for (i = 0; i < R; i++) {
		msgbuf_iter_seek(&iter, msgs[i]);
		ret = msgbuf_iter_valid(&iter);
		ASSERT_EQUAL(1, ret);
		ret = msg_key_compare(msgs[i], &iter.key);
		ASSERT_EQUAL(0, ret);
	}

	/* do msg free */
	for (i = 0; i < R; i++) {
		xfree(msgs[i]->data);
		xfree(msgs[i]);
	}
	xfree(msgs);

	msgbuf_free(bsm);
	xcheck_all_free();
}

CTEST(msgbuf, multiversion)
{
	int i;
	int ret;
	int R = 123;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct msgbuf_iter iter;
	struct msgbuf *bsm = msgbuf_new();
	struct txnid_pair xidpair =  {
		.child_xid = TXNID_NONE,
		.parent_xid = TXNID_NONE
	};

	_random_key(vbuf, VAL_SIZE);

	MSN msn = 0U;
	for (i = 0; i < R; i++) {
		memset(kbuf, 0, KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		msgbuf_put(bsm,
		           msn++,
		           MSG_INSERT,
		           &k,
		           &v,
		           &xidpair
		          );
	}

	/* 3 versions of key-66 */
	R = 3;
	msn = 661;
	for (i = 0; i < R; i++) {
		memset(kbuf, 0, KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-66");

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		msgbuf_put(bsm,
		           msn++,
		           MSG_INSERT,
		           &k,
		           &v,
		           &xidpair
		          );
	}

	/* 5 versions of key-88 */
	R = 5;
	msn = 881;
	for (i = 0; i < R; i++) {
		memset(kbuf, 0, KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-88");

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		msgbuf_put(bsm,
		           msn++,
		           MSG_INSERT,
		           &k,
		           &v,
		           &xidpair
		          );
	}

	/* key-66 */
	memset(kbuf, 0, KEY_SIZE);
	snprintf(kbuf, KEY_SIZE, "key-66");

	struct msg k1 = {.data = kbuf, .size = KEY_SIZE};
	msgbuf_iter_init(&iter, bsm);
	msgbuf_iter_seek(&iter, &k1);
	ret = msgbuf_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(66, iter.msn);

	/* key-66 array iterator */
	ret = msgbuf_internal_iter_next(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(66, iter.msn);
	ret = msgbuf_internal_iter_next(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(661, iter.msn);
	ret = msgbuf_internal_iter_next(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(662, iter.msn);
	ret = msgbuf_internal_iter_next(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(663, iter.msn);


	/* less than valid */
	ret = msgbuf_iter_valid_lessorequal(&iter, &k1);
	ASSERT_EQUAL(1, ret);

	msgbuf_iter_next(&iter);
	ret = msgbuf_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);

	memset(kbuf, 0, KEY_SIZE);
	snprintf(kbuf, KEY_SIZE, "key-68");
	k1.size = KEY_SIZE;
	k1.data = kbuf;
	ret = msgbuf_iter_valid_lessorequal(&iter, &k1);
	ASSERT_EQUAL(1, ret);

	memset(kbuf, 0, KEY_SIZE);
	snprintf(kbuf, KEY_SIZE, "key-65");
	k1.size = KEY_SIZE;
	k1.data = kbuf;
	ret = msgbuf_iter_valid_lessorequal(&iter, &k1);
	ASSERT_EQUAL(0, ret);

	/* key-88 */
	memset(kbuf, 0, KEY_SIZE);
	snprintf(kbuf, KEY_SIZE, "key-88");

	struct msg k2 = {.data = kbuf, .size = KEY_SIZE};
	msgbuf_iter_seek(&iter, &k2);
	ret = msgbuf_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(88, iter.msn);

	ret = msgbuf_internal_iter_next(&iter);
	ASSERT_EQUAL(1, ret);
	ASSERT_EQUAL(88, iter.msn);

	/* key-88 array iterator */
	i = 881;
	while (msgbuf_internal_iter_next(&iter)) {
		ASSERT_EQUAL(i++, iter.msn);
	}

	msgbuf_free(bsm);
	xcheck_all_free();
}
