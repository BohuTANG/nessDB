/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_COMPARE_FUNC_H_
#define nessDB_COMPARE_FUNC_H_

#include "xtypes.h"
#include "internal.h"

int msgbuf_key_compare(void *, void *, int *);
int msg_key_compare(struct msg *, struct msg *);

#endif /* _nessDB_COMPARE_FUNC_H_ */
