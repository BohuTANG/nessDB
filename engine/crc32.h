/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_CRC32_H_
#define nessDB_CRC32_H_

#include "internal.h"

#define CRC_SIZE	(sizeof(uint32_t))

uint32_t crc32(const char *buf, uint32_t n);

#endif /* nessDB_CRC32_H_ */
