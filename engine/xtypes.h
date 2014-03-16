/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_XTYPES_H_
#define nessDB_XTYPES_H_

#include <stdint.h>

/* error no from -30800 to -30999 */
#define	 NESS_ERR		(0)
#define	 NESS_OK		(1)
#define	NESS_INNER_XSUM_ERR	(30999)
#define	NESS_LEAF_XSUM_ERR (-30998)
#define	NESS_PART_XSUM_ERR (-30997)
#define	NESS_HDR_XSUM_ERR (-30996)
#define	NESS_DO_XSUM_ERR (-30995)
#define	NESS_READ_ERR (-30994)
#define	NESS_WRITE_ERR (-30993)
#define	NESS_FSYNC_ERR (-30992)
#define	NESS_SERIAL_BLOCKPAIR_ERR (-30991)
#define	NESS_DESERIAL_BLOCKPAIR_ERR (-30990)
#define	NESS_BLOCK_NULL_ERR (-30989)
#define	NESS_BUF_TO_BSM_ERR (-30988)
#define	NESS_BSM_TO_BUF_ERR (-30987)
#define	NESS_SERIAL_NONLEAF_FROM_BUF_ERR (-30986)
#define	NESS_DESERIAL_NONLEAF_FROM_BUF_ERR (-30985)
#define	NESS_LAYOUT_VERSION_OLDER_ERR (-30984)
#define	NESS_LOG_READ_SIZE_ERR (-30983)
#define	NESS_LOG_READ_DATA_ERR (-30982)
#define	NESS_LOG_READ_XSUM_ERR (-30981)
#define	NESS_LOG_EOF (-30980)

typedef enum {
	SERIALIZABLE,
	REPEATABLE,
	READCOMMITTED,
	READUNCOMMITTED,
} TXN_ISO;

typedef struct txn TXN;

struct msg {
	uint32_t size;
	void *data;
};

#endif
