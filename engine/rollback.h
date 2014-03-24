/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_ROLLBACK_H_
#define nessDB_ROLLBACK_H_

#include "xtypes.h"
#include "internal.h"

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

struct roll_entry {
	struct roll_entry *prev;
	union {
		struct rolltype_cmdinsert cmdinsert;
		struct rolltype_cmddelete cmddelete;
		struct rolltype_cmdupdate cmdupdate;
	} u;
};


#endif /* nessDB_ROLLBACK_H_*/
