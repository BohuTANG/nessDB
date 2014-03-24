/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef __nessDB__DEBUG_H_
#define __nessDB__DEBUG_H_

#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

enum DEBUG_LEVEL {
	LEVEL_DEBUG = 0,
	LEVEL_INFO,
	LEVEL_WARNING,
	LEVEL_ERROR
};

void __debug(char *file,
             int line,
             enum DEBUG_LEVEL level,
             const char *fmt, ...);

void _assert(void *expr, void *filename, unsigned lineno);

#ifdef ASSERT
#define nassert(exp) (void) ((exp) || (_assert(#exp, __FILE__, __LINE__), 0))
#else
#define nassert(exp) ((void) 0)
#endif


#ifdef INFO
#define __INFO(format, ...)						\
	do { __debug(__FILE__, __LINE__,				\
			LEVEL_INFO, format, __VA_ARGS__);		\
	} while (0)
#else
#define __INFO(format, ...) do {} while (0)
#endif

#if defined(DEBUG) || defined(INFO)
#define __DEBUG(format, ...)						\
	do { __debug(__FILE__, __LINE__,				\
			LEVEL_DEBUG,					\
			format, __VA_ARGS__);				\
	} while (0)
#else
#define __DEBUG(format, ...) do {} while (0)
#endif

#if defined(WARN) || defined(INFO)
#define __WARN(format, ...)						\
	do { __debug(__FILE__, __LINE__,				\
			LEVEL_WARNING,					\
			format, __VA_ARGS__);				\
	} while (0)
#else
#define __WARN(format, ...) do {} while (0)
#endif

#if defined(ERROR) || defined(DEBUG) || defined(WARN) || defined(INFO)
#define __ERROR(format, ...)						\
	do { __debug(__FILE__, __LINE__,				\
			LEVEL_ERROR,					\
			format, __VA_ARGS__);				\
	} while (0)
#else
#define __ERROR(format, ...) do {} while (0)
#endif

#define __PANIC(format, ...)						\
	do { __debug(__FILE__, __LINE__,				\
			LEVEL_ERROR,					\
			format, __VA_ARGS__); abort(); exit(1);		\
	} while (0)

#endif /* __nessDB__DEBUG_H_ */
