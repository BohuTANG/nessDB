/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_XTYPES_H_
#define nessDB_XTYPES_H_

#include "internal.h"

struct xids {
	uint8_t num_xids;
	TXID ids[];
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
	return (sizeof(*xids) + sizeof(TXID) * xids->num_xids);
}

static inline uint32_t get_entrylen(struct append_entry *entry)
{
	return sizeof(*entry);
}

#endif
