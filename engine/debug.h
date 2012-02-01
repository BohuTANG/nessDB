/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _DEBUG_H
#define _DEBUG_H

#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

typedef enum{LEVEL_DEBUG = 0, LEVEL_INFO, LEVEL_WARNING, LEVEL_ERROR}DEBUG_LEVEL;
void __DEBUG(DEBUG_LEVEL level, const char *fmt, ...);

#endif
