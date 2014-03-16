/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TXN_H_
#define nessDB_TXN_H_

#include "xtypes.h"
#include "internal.h"

/**
 *
 * @file txn.h
 * @brief transaction
 *
 */

int txn_begin(TXN *parent, int flags, TXN **txn);
int txn_commit(TXN *txn);
int txn_abort(TXN *txn);

#endif /* nessDB_TXN_H_ */
