/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_INTERNAL_H_
#define nessDB_INTERNAL_H_

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
typedef uint64_t TXNID;
typedef uint64_t DISKOFF;

/* compress method */
typedef enum {
	NESS_NO_COMPRESS,
	NESS_QUICKLZ_METHOD,
} ness_compress_method_t;

/* lock type */
enum lock_type {
	L_READ,
	L_WRITE,
};

/* transaction state */
typedef enum txn_state {
	TX_LIVE,
	TX_PREPARING,
	TX_COMMITTING,
	TX_ABORTINT,
} TXNSTATE;

/* transaction id */
struct xids {
	uint8_t num_xids;
	TXNID ids[];
} __attribute__((__packed__));

struct append_entry {
	uint32_t keylen;
	uint32_t vallen;
	uint8_t type;
	MSN msn;
} __attribute__((__packed__));

static inline uint32_t get_xidslen(struct xids *xids)
{
	if (!xids) return 0;
	return (sizeof(*xids) + sizeof(TXNID) * xids->num_xids);
}

static inline uint32_t get_entrylen(struct append_entry *entry)
{
	return sizeof(*entry);
}

/*
 * align to ALIGNMENT for direct I/O
 * but it is not always 512 bytes:
 * http://people.redhat.com/msnitzer/docs/io-limits.txt
 */
#define ALIGNMENT (512)
static inline uint64_t ALIGN(uint64_t v)
{
	return (v + ALIGNMENT - 1)&~(ALIGNMENT - 1);
}

#endif /* nessDB_INTERNAL_H_ */
