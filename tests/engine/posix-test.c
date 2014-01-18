/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "posix.h"
#include "ctest.h"

static void *run_100ms(void *arg)
{
	uint32_t *c = (uint32_t*)arg;
	*c += 1;
	usleep(100000); // 100ms

	return NULL;
}

CTEST(cron_test, start) {
	uint32_t c = 0;
	struct cron *cron = cron_new(run_100ms, 100);

	cron_start(cron, &c);
	usleep(550000); // 500ms
	cron_stop(cron);

	ASSERT_EQUAL(3, c);

	cron_free(cron);
	xcheck_all_free();
}

static void *run_0sec(void *arg)
{
	uint32_t *c = (uint32_t*)arg;
	*c += 1;

	return NULL;
}

CTEST(cron_test, change_period) {
	uint32_t c = 0;
	struct cron *cron = cron_new(run_0sec, 1000000);

	cron_start(cron, &c);

	cron_change_period(cron, 100);
	usleep(350000); // 300ms
	cron_stop(cron);

	ASSERT_EQUAL(3, c);

	cron_free(cron);
	xcheck_all_free();
}

CTEST(cron_test, signal) {
	uint32_t c = 0;
	struct cron *cron = cron_new(run_0sec, 100000000);

	cron_start(cron, &c);

	usleep(1000); // 10ms

	cron_signal(cron);
	usleep(1000); // 10ms

	ASSERT_EQUAL(1, c);

	cron_stop(cron);
	cron_free(cron);
	xcheck_all_free();
}
