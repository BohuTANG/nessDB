/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _UTIL_H
#define _UTIL_H

#include <stdint.h>
#define SST_NSIZE (16)
#define SST_FLEN (1024)
#define INDEX_NSIZE (1024)
#define LOG_NSIZE (1024)

struct slice{
	char *data;
	int len;
};

/* Get H bit */
static inline int GET64_H(uint64_t x)
{
	if(((x>>63)&0x01)!=0x01)
		return 0;
	else
		return 1;
}

/* Set H bit to 0 */
static inline uint64_t SET64_H_0(uint64_t x)
{
	return	x&=0x3FFFFFFFFFFFFFFF;
}

/* Set H bit to 1 */
static inline uint64_t SET64_H_1(uint64_t x)
{
	return  x|=0x8000000000000000;	
}

void _ensure_dir_exists(const char *path);
#endif
