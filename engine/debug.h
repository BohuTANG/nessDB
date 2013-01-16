#ifndef __nessDB_DEBUG_H
#define __nessDB_DEBUG_H

#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

typedef enum {LEVEL_DEBUG = 0, LEVEL_INFO, LEVEL_WARNING, LEVEL_ERROR} DEBUG_LEVEL;

void __debug(char *file, int line, DEBUG_LEVEL level, const char *fmt, ...);

void __DEBUG_INIT_SIGNAL();

#ifdef INFO
	#define __INFO(format, args...)\
		do { __debug(__FILE__, __LINE__, LEVEL_INFO, format, ##args); } while (0)
#else
	#define __INFO(format, args...) do {} while(0)
#endif

#if defined(DEBUG) || defined(INFO)
	#define __DEBUG(format, args...)\
		do { __debug(__FILE__, __LINE__, LEVEL_DEBUG, format, ##args); } while (0)
#else
	#define __DEBUG(format, args...) do {} while(0)
#endif

#if defined(WARN) || defined(INFO)
	#define __WARN(format, args...)\
		do { __debug(__FILE__, __LINE__, LEVEL_WARNING, format, ##args); } while (0)
#else
	#define __WARN(format, args...) do {} while(0)
#endif

#if defined(ERROR) || defined(DEBUG) || defined(WARN) || defined(INFO)
#define __ERROR(format, args...)\
	do { __debug(__FILE__, __LINE__, LEVEL_ERROR, format, ##args); } while (0)
#else
	#define __ERROR(format, args...) do {} while(0)
#endif

#define __PANIC(format, args...)\
	do { __debug(__FILE__, __LINE__, LEVEL_ERROR, format, ##args); abort(); exit(1); } while (0)

#endif
