/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_KIBBUTZ_H_
#define nessDB_KIBBUTZ_H_

#include "internal.h"

struct kibbutz;
struct kibbutz *kibbutz_new(int n_workers);
void kibbutz_enq(struct kibbutz *k, void (*f)(void*), void *extra);
void kibbutz_free(struct kibbutz *k);

#endif
