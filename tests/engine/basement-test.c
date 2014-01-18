/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "basement.h"
#include "ctest.h"

CTEST(basement, empty)
{
	int ret;
	struct basement_iter iter;
	struct basement *bsm = basement_new();

	basement_iter_init(&iter, bsm);
	basement_iter_seektofirst(&iter);
	ret = basement_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	struct msg m1 = {
		.size = 6,
		.data = "key-01"
	};

	basement_iter_seek(&iter, &m1);
	ret = basement_iter_valid(&iter);
	ASSERT_EQUAL(0, ret);

	basement_free(bsm);
	xcheck_all_free();
}

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

#define KEY_SIZE (16)
#define VAL_SIZE (100)
CTEST(basement, insert_and_lookup)
{
	int i;
	int ret;
	int R = 1000;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct basement_iter iter;
	struct basement *bsm = basement_new();
	struct msg **msgs = xcalloc(R, sizeof(*msgs));

	_random_key(vbuf, VAL_SIZE);
	for (i = 0; i < R; i++) {
		_random_key(kbuf, KEY_SIZE);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		msgs[i] = msgdup(&k);
		basement_put(bsm, &k, &v, MSG_PUT);
	}

	basement_iter_init(&iter, bsm);
	basement_iter_seektolast(&iter);
	ret = basement_iter_valid(&iter);
	ASSERT_EQUAL(1, ret);

	for (i = 0; i < R; i++) {
		basement_iter_seek(&iter, msgs[i]);
		ret = basement_iter_valid(&iter);
		ASSERT_EQUAL(1, ret);
		ret = msgcmp(msgs[i], &iter.key);
		ASSERT_EQUAL(0, ret);
	}

	/* do msg free */
	for (i = 0; i < R; i++) {
		xfree(msgs[i]->data);
		xfree(msgs[i]);
	}
	xfree(msgs);

	basement_free(bsm);
	xcheck_all_free();
}
