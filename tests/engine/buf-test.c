/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include <unistd.h>
#include <stdlib.h>

#include "ctest.h"
#include "internal.h"
#include "buf.h"

CTEST_DATA(buf_test)
{
	struct buffer *b;
};

CTEST_SETUP(buf_test)
{
	data->b = buf_new(1 << 20);
	ASSERT_NOT_NULL(data->b);
}

CTEST_TEARDOWN(buf_test)
{
	buf_free(data->b);
}

CTEST2(buf_test, mix)
{
	uint64_t v1 = 123456789101112, v2;
	buf_putnstr(data->b, "nessdata", 8);
	buf_putuint32(data->b, 0);
	buf_putuint32(data->b, 1);
	buf_putuint64(data->b, v1);

	buf_skip(data->b, 8);

	uint32_t v;
	buf_getuint32(data->b, &v);
	ASSERT_EQUAL(0, v);

	buf_getuint32(data->b, &v);
	ASSERT_EQUAL(1, v);

	buf_getuint64(data->b, &v2);
	ASSERT_EQUAL(v1, v2);
}

CTEST2(buf_test, checksum)
{

	buf_clear(data->b);

	uint32_t cs1, cs2, cs3;
	uint64_t v1 = 123456789101112;
	buf_putnstr(data->b, "nessdata", 8);
	buf_putuint32(data->b, 0);
	buf_putuint32(data->b, 1);
	buf_putuint64(data->b, v1);

	buf_xsum(data->b->buf, data->b->NUL, &cs1);
	buf_putuint32(data->b, cs1);

	buf_skip(data->b, 8 + 4 + 4 + 8);
	buf_getuint32(data->b, &cs2);
	buf_xsum(data->b->buf, 8 + 4 + 4 + 8, &cs3);

	ASSERT_EQUAL(cs2, cs3);
}
