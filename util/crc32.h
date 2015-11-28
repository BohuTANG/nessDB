/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_CRC32_H_
#define nessDB_CRC32_H_

#define CRC_SIZE	(sizeof(uint32_t))

uint16_t crc16(const char *buf, int n);

static inline int do_xsum(const char *data, uint32_t len, uint32_t *xsum)
{
	if (len == 0)
		return 0;

	*xsum = crc16(data, len);

	return 1;
}

#endif /* nessDB_CRC32_H_ */
