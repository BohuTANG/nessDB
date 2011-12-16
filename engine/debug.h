/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _DEBUG_H
#define _DEBUG_H

#include <sys/types.h>
#include <unistd.h>

#ifdef DEBUG
#define __DEBUG(x...) do {                                  								\
        fprintf(stderr, "[%d]	%s(line:%d)	", (int)getpid(),  __FUNCTION__, __LINE__); 	\
        fprintf(stderr, ##x);                               								\
		fprintf(stderr, "\n");																\
    } while(0)
#else
	#define __DEBUG(x...)
#endif

#endif
