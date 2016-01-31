/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_ROLLBACK_H_
#define nessDB_ROLLBACK_H_

struct rolltype_cmdinsert {
	int filenum;
	struct msg *key;
};

struct rolltype_cmddelete {
	int filenum;
	struct msg *key;
};

struct rolltype_cmdupdate {
	int filenum;
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

void rollback_save_cmdinsert(TXN *txn, int fn, struct msg *key);
void rollback_save_cmddelete(TXN *txn, int fn, struct msg *key);
void rollback_save_cmdupdate(TXN *txn, int fn, struct msg *key);

#endif /* nessDB_ROLLBACK_H_*/
