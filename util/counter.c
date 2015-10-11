/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "counter.h"

struct counter *counter_new(int cpus) {
	struct counter *c = xcalloc(1, sizeof(*c));

	c->cpus = cpus;
	c->per_cpu_counter = xcalloc(cpus, sizeof(uint64_t));

	return c;
}

void counter_free(struct counter *c)
{
	xfree(c->per_cpu_counter);
	xfree(c);
}


void counter_incr(struct counter *c)
{
#ifdef HAVE_SCHED_GETCPU 
	int cpu = sched_getcpu();
#else
	int cpu = 4;
#endif

	nassert(cpu < c->cpus);
	c->per_cpu_counter[cpu]++;
}

uint64_t counter_all(struct counter *c)
{
	int i;
	uint64_t all = 0UL;

	for (i = 0; i < c->cpus; i++) {
		all += c->per_cpu_counter[i];
	}

	return all;
}
