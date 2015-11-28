/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "x.h"
#include "t.h"

int rolltree_put(struct rolltree *t,
                 struct msg *k,
                 struct msg *v,
                 msgtype_t type,
                 TXN *txn)
{
	(void)t;
	(void)k;
	(void)v;
	(void)type;
	(void)txn;

	return -1;
}

void rolltree_free(struct rolltree *t)
{
	if (!t) return;

	xfree(t);
}
