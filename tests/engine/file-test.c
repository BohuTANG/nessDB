/*
 * Copyright (c) 2012-2014 The nessDB Authors. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * Author: BohuTANG <overred.shuttler@gmail.com>
 *
 */

#include "buf.h"
#include "file.h"
#include "xmalloc.h"
#include "ctest.h"

CTEST(file, directio) {
	int r;
	int fd;
	char *datas = (char*)xmalloc_aligned(512, 1024);
	const char *dbname = "file-test.brt";

	fd = ness_os_open_direct(dbname, O_CREAT|O_RDWR, 0777);
	ASSERT_TRUE(fd > 0);
	r = ness_os_pwrite(fd, datas, 512, 512UL);
	ASSERT_EQUAL(0, r);

	xfree(datas);
	ness_os_close(fd);
}
