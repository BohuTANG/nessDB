/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _PLATFORM_H
#define _PLATFORM_H

#ifndef O_BINARY
	#define O_BINARY (0) 
#endif

#if defined(__linux__)
	# define open open64
	# define lseek lseek64
	# define fstat fstat64
	# define LSM_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE)
	# define LSM_OPEN_FLAGS   (O_RDWR | O_BINARY | O_LARGEFILE)
#else
	# define open64 open
	# define LSM_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY)
	# define LSM_OPEN_FLAGS   (O_RDWR | O_BINARY)
#endif

#endif
