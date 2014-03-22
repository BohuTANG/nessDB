/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "logw.h"
#include "logr.h"
#include "ctest.h"

#define R (100000)
#define KEY_SIZE (16)
#define VAL_SIZE (100)

CTEST(log, write) {
	int i;
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];

	uint32_t tbn = 0U;
	uint64_t lsn = 112;
	struct options *opts = options_new();
	struct logw *lgw = logw_open(opts, lsn);

	for (i = 0; i < R; i++) {
		memset(kbuf, 0 , KEY_SIZE);
		memset(vbuf, 0 , VAL_SIZE);
		snprintf(kbuf, KEY_SIZE, "key-%d", i);
		snprintf(vbuf, VAL_SIZE, "val-%d", i);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};

		logw_append(lgw, &k, &v, MSG_INSERT, tbn);
	}

	uint32_t all_size;
	uint32_t one_size = 4	/* first length */
		+ 4		/* tid */
		+ 4		/* key length */
		+ KEY_SIZE	/* key */
		+ 4		/*val length */
		+ VAL_SIZE	/* val */
		+ 4;		/* crc */

	all_size = R * one_size;
	ASSERT_EQUAL(all_size, lgw->size);

	logw_close(lgw);
	options_free(opts);
}

CTEST(log, read) {
	uint64_t lsn = 112;
	struct options *opts = options_new();
	struct logr *lgr = logr_open(opts, lsn);

	struct msg k;
	struct msg v;
	msgtype_t type;
	uint32_t tid;

	int i = 0;
	while (logr_read(lgr, &k, &v, &type, &tid) == NESS_OK) {
		i++;
	}

	ASSERT_EQUAL(R, i);

	logr_close(lgr);
	options_free(opts);
}
