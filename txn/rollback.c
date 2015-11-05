/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "x.h"

void rollentry_free(struct roll_entry *re)
{
	switch (re->type) {
	case RT_CMDINSERT:
		msgfree(re->u.cmdinsert.key);
		xfree(re);
		break;
	case RT_CMDDELETE:
		msgfree(re->u.cmddelete.key);
		xfree(re);
		break;
	case RT_CMDUPDATE:
		msgfree(re->u.cmdupdate.key);
		xfree(re);
		break;
	}
}

void rollback_save_cmdinsert(TXN *txn, int fn, struct msg *key)
{
	(void)txn;
	(void)fn;
	(void)key;
}

void rollback_save_cmddelete(TXN *txn, int fn, struct msg *key)
{
	(void)txn;
	(void)fn;
	(void)key;
}

void rollback_save_cmdupdate(TXN *txn, int fn, struct msg *key)
{
	(void)txn;
	(void)fn;
	(void)key;
}
