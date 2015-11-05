/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_COUNTER_H_
#define nessDB_COUNTER_H_

struct counter {
	int cpus;
	uint64_t *per_cpu_counter;
};

struct counter *counter_new(int cpus);
void counter_free(struct counter *);

void counter_incr(struct counter *);
uint64_t counter_all(struct counter *);

#endif /* nessDB_COUNTER_H_ */
