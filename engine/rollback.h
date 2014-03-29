/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_ROLLBACK_H_
#define nessDB_ROLLBACK_H_

#include "xtypes.h"
#include "internal.h"
#include "msg.h"
#include "xmalloc.h"

struct rolltype_cmdinsert {
	FILENUM filenum;
	struct msg *key;
};

struct rolltype_cmddelete {
	FILENUM filenum;
	struct msg *key;
};

struct rolltype_cmdupdate {
	FILENUM filenum;
	struct msg *key;
};

enum rollback_type {
	RT_CMDINSERT = 'i',
	RT_CMDDELETE = 'd',
	RT_CMDUPDATE = 'u'
};

struct roll_entry {
	int isref;
	enum rollback_type type;
	struct roll_entry *prev;
	union {
		struct rolltype_cmdinsert cmdinsert;
		struct rolltype_cmddelete cmddelete;
		struct rolltype_cmdupdate cmdupdate;
	} u;
};

void rollentry_free(struct roll_entry *re);

void rollback_save_cmdinsert(TXN *txn, FILENUM fn, struct msg *key);
void rollback_save_cmddelete(TXN *txn, FILENUM fn, struct msg *key);
void rollback_save_cmdupdate(TXN *txn, FILENUM fn, struct msg *key);

#endif /* nessDB_ROLLBACK_H_*/
