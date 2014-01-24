/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_RANDOM_H_
#define nessDB_RANDOM_H_

#include "internal.h"

struct random {
	uint32_t seed;
	uint32_t pos;
	uint32_t size;
	char *buffer;
};

struct random *rnd_new();
uint32_t rnd_next(struct random*);
char *rnd_str(struct random*, int len);
void rnd_free(struct random*);

#endif /* nessDB_RANDOM_H_ */


