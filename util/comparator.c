/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"

int bt_compare_func_builtin(void *a,
                            int alen,
                            void *b,
                            int blen)
{
	int r;
	uint32_t minlen = alen < blen ? alen : blen;

	r = memcmp(a, b, minlen);
	if (r == 0)
		return a - b;
	return r;
}
