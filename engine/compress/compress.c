/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "debug.h"
#include "compress.h"
#include "compress/quicklz.h"

uint32_t ness_compress_bound(ness_compress_method_t m, uint32_t size)
{
	switch (m) {
	case NESS_NO_COMPRESS:
		return size + 1;
	case NESS_QUICKLZ_METHOD:
		return size + 400 + 1;
	default: break;
	}

	return 0;
}

void ness_compress(ness_compress_method_t m,
		const char *src,
		uint32_t src_size,
		char *dst,
		uint32_t *dst_size)
{
	switch (m) {
	case NESS_NO_COMPRESS:
		memcpy(dst + 1, src, src_size);
		*dst_size = src_size + 1;
		dst[0] = NESS_NO_COMPRESS;
		return;

	case NESS_QUICKLZ_METHOD:
		if (src_size == 0) {
			/*
			 * quicklz requirs at least one byte
			 */
			*dst_size = 1;
		} else {
			uint32_t act_dstlen;
			qlz_state_compress *qsc;

			qsc = xcalloc(1, sizeof(*qsc));
			act_dstlen = qlz_compress(src, dst + 1, src_size, qsc);
			*dst_size = act_dstlen + 1;
			xfree(qsc);
		}

		dst[0] = NESS_QUICKLZ_METHOD + (QLZ_COMPRESSION_LEVEL << 4);
		return;
	}
}

void ness_decompress(const char *src,
		uint32_t src_size,
		char *dst,
		uint32_t dst_size)
{
	switch (src[0] & 0xF) {
	case NESS_NO_COMPRESS:
		memcpy(dst, src + 1, src_size - 1);
		break;

	case NESS_QUICKLZ_METHOD: {
		uint32_t raw_size;
		qlz_state_decompress *qsd;

		qsd = xcalloc(1, sizeof(*qsd));
		raw_size = qlz_decompress(src + 1, dst, qsd);
		nassert(raw_size == dst_size);
		(void)raw_size;
		(void)dst_size;
      
		xfree(qsd);
	}
		break;
	default:
		printf("no decompress support!\n");
		nassert(1);
		break;
	}
}
