/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_ATOMIC_H_
#define nessDB_ATOMIC_H_

#include <stdint.h>

/* atomic */
int atomic32_increment(int *dest);
int atomic32_decrement(int *dest);

uint64_t atomic64_increment(uint64_t *dest);
uint64_t atomic64_decrement(uint64_t *dest);

#endif /* nessDB_ATOMIC_H_ */
