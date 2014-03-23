/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_COMPARE_FUNC_H_
#define nessDB_COMPARE_FUNC_H_

#include "xtypes.h"
#include "internal.h"

int internal_key_compare(void *a, void *b);
int msg_key_compare(struct msg *a, struct msg *b);

#endif /* _nessDB_COMPARE_FUNC_H_ */
