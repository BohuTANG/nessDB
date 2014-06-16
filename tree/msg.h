/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MSG_H_
#define nessDB_MSG_H_

#include "xtypes.h"
#include "internal.h"

/* msg type */
typedef enum {
	MSG_NONE = 0,
	MSG_INSERT = 1,
	MSG_DELETE = 2,
	MSG_UPDATE = 3,
	MSG_COMMIT = 4,
	MSG_ABORT = 5
} msgtype_t;

/* buffer tree cmd */
struct bt_cmd {
	MSN msn;
	msgtype_t type;
	struct msg *key;
	struct msg *val;
	struct txnid_pair xidpair;
};

static inline int msg_key_compare(struct msg *a, struct msg *b)
{
	if (!a) return -1;
	if (!b) return 1;

	int r;
	uint32_t minlen = a->size < b->size ? a->size : b->size;
	r = memcmp(a->data, b->data, minlen);
	if (r == 0)
		return (a->size - b->size);

	return r;
}

struct msg *msgdup(struct msg *src);
void msgcpy(struct msg *dst, struct msg *src);
uint32_t msgsize(struct msg *msg);
void msgfree(struct msg *msg);

#endif /* nessDB_MSG_H_ */
