/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MSG_H_
#define nessDB_MSG_H_

#include "internal.h"

#define MSGTYPE_SIZE	(2)
/* this enum type is short(uint16_t) */
typedef enum {
	MSG_NONE = 0,
	MSG_PUT = 1,
	MSG_DEL = 2
} msgtype_t;

struct msg {
	uint32_t size;
	void *data;
};

struct msg *msgdup(struct msg *src);
void msgcpy(struct msg *dst, struct msg *src);
uint32_t msgsize(struct msg *msg);
void msgfree(struct msg *msg);

#endif /* nessDB_MSG_H_ */
