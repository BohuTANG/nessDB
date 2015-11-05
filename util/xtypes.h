/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_XTYPES_H_
#define nessDB_XTYPES_H_

/* error no from -30800 to -30999 */
#define	NESS_ERR				(0)
#define	NESS_OK				  	(1)
#define	NESS_INNER_XSUM_ERR			(-30999)
#define	NESS_BAD_XSUM_ERR			(-30998)
#define	NESS_PART_XSUM_ERR			(-30997)
#define	NESS_HDR_XSUM_ERR			(-30996)
#define	NESS_DO_XSUM_ERR			(-30995)
#define	NESS_READ_ERR				(-30994)
#define	NESS_WRITE_ERR				(-30993)
#define	NESS_FSYNC_ERR				(-30992)
#define	NESS_SERIAL_BLOCKPAIR_ERR		(-30991)
#define	NESS_DESERIAL_BLOCKPAIR_ERR		(-30990)
#define	NESS_BLOCK_NULL_ERR			(-30989)
#define	NESS_BUF_TO_MSGBUF_ERR	    		(-30988)
#define	NESS_MSGBUF_TO_BUF_ERR			(-30987)
#define	NESS_SERIAL_NONLEAF_FROM_BUF_ERR	(-30986)
#define	NESS_DESERIAL_NONLEAF_FROM_BUF_ERR	(-30985)
#define	NESS_LAYOUT_VERSION_OLDER_ERR		(-30984)
#define	NESS_LOG_READ_SIZE_ERR			(-30983)
#define	NESS_LOG_READ_DATA_ERR			(-30982)
#define	NESS_LOG_READ_XSUM_ERR			(-30981)
#define	NESS_LOG_EOF				(-30980)
#define	NESS_TXN_COMMIT_ERR			(-30979)
#define	NESS_TXN_ABORT_ERR			(-30978)
#define	NESS_NOTFOUND				(-30978)


#define nesslikely(EXPR) __builtin_expect(!! (EXPR), 1)
#define nessunlikely(EXPR) __builtin_expect(!! (EXPR), 0)

/* reserved 1024bytes for double-write tree header */
#define BLOCK_OFFSET_START (ALIGN(1024))

/* reserved NID for block-self using */
#define NID_START (3)
#define FILE_NAME_MAXLEN (256)
#define ZERO_MSN (0)

typedef uint64_t NID;
typedef uint64_t MSN;
typedef uint64_t TXNID;
typedef uint64_t DISKOFF;

/* lock type */
enum lock_type {
    L_NONE = 0,
    L_READ = 1,
    L_WRITE = 2,
};

/* transaction state */
typedef enum txn_state {
    TX_LIVE,
    TX_PREPARING,
    TX_COMMITTING,
    TX_ABORTINT,
} TXNSTATE;

struct txnid_pair {
	TXNID child_xid;
	TXNID parent_xid;
};

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



/* compress method */
typedef enum {
    NESS_NO_COMPRESS = 0,
    NESS_SNAPPY_METHOD = 1,
} ness_compress_method_t;

/* txn */
#define TXNID_NONE (0)
typedef enum {
    TXN_ISO_SERIALIZABLE = 0,
    TXN_ISO_REPEATABLE = 1,
    TXN_ISO_READ_COMMITTED = 2,
    TXN_ISO_READ_UNCOMMITTED = 3
} TXN_ISOLATION_TYPE;

/*
types of snapshots that can be taken by a txn
   - TXN_SNAPSHOT_NONE: means that there is no snapshot. Reads do not use snapshot reads.
			used for SERIALIZABLE and READ UNCOMMITTED
   - TXN_SNAPSHOT_ROOT: means that all txns use their root transaction's snapshot
			used for REPEATABLE READ
   - TXN_SNAPSHOT_CHILD: means that each child txn creates its own snapshot
			used for READ COMMITTED
*/
typedef enum {
    TXN_SNAPSHOT_NONE = 0,
    TXN_SNAPSHOT_ROOT = 1,
    TXN_SNAPSHOT_CHILD = 2
} TXN_SNAPSHOT_TYPE;

typedef struct txn TXN;
typedef struct logger LOGGER;
typedef struct {
	uint32_t fileid;
} FILENUM;

struct msg {
	uint32_t size;
	void *data;
};

#endif /* nessDB_XTYPES_H_ */
