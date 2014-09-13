/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "counter.h"
#include "../ctest.h"

CTEST(counter, inrc)
{
	struct counter *c = counter_new(128);

	counter_incr(c);
	counter_incr(c);

	uint64_t all = counter_all(c);
	ASSERT_EQUAL(2, all);

	counter_free(c);
}
