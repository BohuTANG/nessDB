/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CRC32_H_
#define nessDB_CRC32_H_

#define CRC_SIZE	(sizeof(uint32_t))

uint32_t crc32(const char *buf, uint32_t n);

static inline int do_xsum(const char *data, uint32_t len, uint32_t *xsum)
{
	if (len == 0)
		return 0;

	*xsum = crc32(data, len);

	return 1;
}

#endif /* nessDB_CRC32_H_ */
