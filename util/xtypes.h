/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_XTYPES_H_
#define nessDB_XTYPES_H_

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
