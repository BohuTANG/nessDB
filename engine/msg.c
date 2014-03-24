/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "msg.h"

struct msg *msgdup(struct msg *src) {
	struct msg *dst;

	if (!src)
		return NULL;

	dst = xcalloc(1, sizeof(*dst));
	dst->size = src->size;
	dst->data = xcalloc(1, dst->size + 1);
	memcpy(dst->data, src->data, src->size);

	return dst;
}

void msgcpy(struct msg *dst, struct msg *src)
{
	dst->size = src->size;
	dst->data = xcalloc(src->size + 1, sizeof(char));
	memcpy(dst->data, src->data, src->size);
}

uint32_t msgsize(struct msg *msg)
{
	return (sizeof(msg->size) + msg->size);
}

void msgfree(struct msg *msg)
{
	if (!msg) return;

	xfree(msg->data);
	xfree(msg);
}
