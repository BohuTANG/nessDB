/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "compress.h"
#include "compress/snappy.h"

uint32_t ness_compress_bound(ness_compress_method_t m, uint32_t size)
{
	switch (m) {
	case NESS_NO_COMPRESS:
		return size + 1;
	case NESS_SNAPPY_METHOD:
		return snappy_max_compressed_length(size) + 1;
	default:
		break;
	}

	return 0;
}

int ness_compress(ness_compress_method_t m,
                  const char *src,
                  uint32_t src_size,
                  char *dst,
                  uint32_t *dst_size)
{
	int ret = NESS_OK;

	switch (m) {
	case NESS_NO_COMPRESS:
		memcpy(dst + 1, src, src_size);
		*dst_size = src_size + 1;
		dst[0] = NESS_NO_COMPRESS;
		break;

	case NESS_SNAPPY_METHOD:
		if (src_size == 0) {
			*dst_size = 1;
		} else {
			size_t out_size;
			int status;
			struct snappy_env env;
			snappy_init_env(&env);

			status = snappy_compress(&env, src, src_size, dst + 1, &out_size);
			snappy_free_env(&env);
			if (status != 0) {
				__ERROR("snappy compress error %d, src_size %d, dst_size %d",
				        status,
				        src_size,
				        dst_size);
				ret = 0;
			}
			*dst_size = out_size + 1;
		}

		dst[0] = NESS_SNAPPY_METHOD;
		break;

	default:
		ret = 0;
		__ERROR("%s", "no compress method support!");
		break;
	}

	return ret;
}

int ness_decompress(const char *src,
                    uint32_t src_size,
                    char *dst,
                    uint32_t dst_size)
{
	int ret = NESS_OK;

	/* compressed data is NULL */
	if (src_size == 1)
		return NESS_ERR;

	switch (src[0] & 0xF) {
	case NESS_NO_COMPRESS:
		memcpy(dst, src + 1, src_size - 1);
		break;

	case NESS_SNAPPY_METHOD: {
			int status;
			struct snappy_env env;
			snappy_init_env(&env);

			status = snappy_uncompress(src + 1, src_size - 1, dst);
			snappy_free_env(&env);
			if (status != 0) {
				__ERROR("snappy uncompress error %d", status);
				ret = 0;
				goto ERR;
			}
			(void)dst_size;
		}
		break;
	default:
		ret = 0;
		__ERROR("%s", "no decompress method support!");
		break;
	}

ERR:
	return ret;
}
