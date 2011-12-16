/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _UTIL_H
#define _UTIL_H

#define SST_NSIZE (16)
#define SST_FLEN (1024)
#define INDEX_NSIZE (1024)
#define LOG_NSIZE (1024)

struct slice{
	char *data;
	int len;
};

void _ensure_dir_exists(const char *path);
#endif
