/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "compress/compress.h"
#include "ctest.h"

const char *data = "i am nessDB, BohuTANG eggs me....";

CTEST(compress, compressed)
{
	const uint32_t data_size = strlen(data);
	uint32_t bound_size = ness_compress_bound(NESS_NO_COMPRESS, data_size);

	char *src = xcalloc(1, data_size + 1);
	char *dst = xmalloc(bound_size);
	uint32_t dst_size = 0;

	ness_compress(NESS_NO_COMPRESS, data, data_size, dst, &dst_size);

	ness_decompress(dst, dst_size, src, data_size);
	ASSERT_STR(data, src);
	xfree(dst);
	xfree(src);

	bound_size = ness_compress_bound(NESS_SNAPPY_METHOD, data_size);
	dst = xmalloc(bound_size);
	ness_compress(NESS_SNAPPY_METHOD, data, data_size, dst, &dst_size);
	src = xcalloc(1, data_size + 1);
	ness_decompress(dst, dst_size, src, data_size);
	ASSERT_STR(data, src);
	xfree(dst);
	xfree(src);
	xcheck_all_free();
}
