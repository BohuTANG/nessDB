/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "logw.h"
#include "txn.h"

struct txn {
	struct logw *lgw;
	struct txn *parent;
	struct txn *child;
};

int txn_begin(TXN *parent, int flags, TXN **txn)
{
	(void)parent;
	(void)flags;
	struct txn *tx;
	
	tx = xcalloc(1, sizeof(struct txn));
	*txn = tx;

	return NESS_ERR;
}

int txn_commit(TXN *txn)
{
	(void)txn;
	return NESS_ERR;
}

int txn_abort(TXN *txn)
{
	(void)txn;
	return NESS_ERR;
}
