/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_COMPRESS_H_
#define nessDB_COMPRESS_H_

typedef enum {
	NESS_NO_COMPRESS = 0,
	NESS_SNAPPY_METHOD = 1,
} ness_compress_method_t;

uint32_t ness_compress_bound(ness_compress_method_t m, uint32_t size);

int ness_compress(ness_compress_method_t m,
                  const char *src,
                  uint32_t src_size,
                  char *dst,
                  uint32_t *dst_size);

int ness_decompress(const char *src,
                    uint32_t src_size,
                    char *dst,
                    uint32_t dst_size);

#endif /* nessDB_COMPRESS_H_ */

