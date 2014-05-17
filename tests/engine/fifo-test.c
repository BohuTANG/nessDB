/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "fifo.h"
#include "compare-func.h"
#include "debug.h"
#include "ctest.h"

CTEST(fifo, empty)
{
	struct fifo_iter iter;
	struct fifo *fifo = fifo_new();

	fifo_iter_init(&iter, fifo);

	fifo_free(fifo);
	xcheck_all_free();
}

#define KEY_SIZE (16)
#define VAL_SIZE (100)
CTEST(fifo, insert_and_iter)
{
	int i;
	int ret;
	int R = 1000;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	struct fifo_iter iter;
	struct fifo *fifo = fifo_new();
	struct msg **msgs = xcalloc(R, sizeof(*msgs));
	struct txnid_pair xidpair =  {
		.child_xid = TXNID_NONE,
		.parent_xid = TXNID_NONE
	};

	MSN msn = 0U;

	for (i = 0; i < R; i++) {
		memset(kbuf, 0, KEY_SIZE);
		memset(vbuf, 0, VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		msgs[i] = msgdup(&k);
		fifo_append(fifo,
		            msn++,
		            MSG_INSERT,
		            &k,
		            &v,
		            &xidpair
		           );
	}

	fifo_iter_init(&iter, fifo);

	i = 0;
	while (fifo_iter_next(&iter)) {
		ret = msg_key_compare(msgs[i], &iter.key);
		ASSERT_EQUAL(0, ret);
		i++;
	}
	ASSERT_EQUAL(R, i);

	/* do msg free */
	for (i = 0; i < R; i++) {
		xfree(msgs[i]->data);
		xfree(msgs[i]);
	}
	xfree(msgs);

	fifo_free(fifo);
	xcheck_all_free();
}
