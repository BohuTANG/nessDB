/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_INTERNAL_H_
#define nessDB_INTERNAL_H_
#if defined(__linux__)
#define _GNU_SOURCE
#endif

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#ifndef O_BINARY
#define O_BINARY (0)
#endif

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include "xmalloc.h"

/* reserved 1024bytes for double-write tree header */
#define BLOCK_OFFSET_START (ALIGN(1024))

/* reserved NID for block-self using */
#define NID_START (3)
#define FILE_NAME_MAXLEN (256)

typedef uint64_t NID;
typedef uint64_t MSN;
typedef uint64_t TXID;
typedef uint64_t DISKOFF;

typedef enum {
	NESS_NO_COMPRESS,
	NESS_QUICKLZ_METHOD
} ness_compress_method_t;

/* error no from -30800 to -30999 */
typedef enum {
	NESS_ERR = 0,
	NESS_OK = 1,
	NESS_INNER_XSUM_ERR = -30999,
	NESS_LEAF_XSUM_ERR = -30998,
	NESS_PART_XSUM_ERR = -30997,
	NESS_HDR_XSUM_ERR = -30996,
	NESS_DO_XSUM_ERR = -30995,
	NESS_READ_ERR = -30994,
	NESS_WRITE_ERR = -30993,
	NESS_FSYNC_ERR = -30992,
	NESS_SERIAL_BLOCKPAIR_ERR = -30991,
	NESS_DESERIAL_BLOCKPAIR_ERR = -30990,
	NESS_BLOCK_NULL_ERR = -30989,
	NESS_BUF_TO_BSM_ERR = -30988,
	NESS_BSM_TO_BUF_ERR = -30987,
	NESS_SERIAL_NONLEAF_FROM_BUF_ERR = -30986,
	NESS_DESERIAL_NONLEAF_FROM_BUF_ERR = -30985,
	NESS_LAYOUT_VERSION_OLDER_ERR = -30984,
	NESS_LOG_READ_SIZE_ERR = -30983,
	NESS_LOG_READ_DATA_ERR = -30982,
	NESS_LOG_READ_XSUM_ERR = -30981,
	NESS_LOG_EOF = -30980,
} ness_errno_t;

/*
 * align to ALIGNMENT for direct I/O
 * but it is not always 512 bytes:
 * http://people.redhat.com/msnitzer/docs/io-limits.txt
 */
#define ALIGNMENT (512)
static inline uint64_t ALIGN(uint64_t v)
{
	return  (v + ALIGNMENT - 1)&~(ALIGNMENT - 1);
}

#endif /* nessDB_INTERNAL_H_ */
