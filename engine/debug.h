#ifndef _DEBUG_H
#define _DEBUG_H

#include <sys/types.h>
#include <unistd.h>
#include <err.h>

#ifdef DEBUG
    #define __DEBUG(fmt, ...) warnx("[%d]	%s(line:%d)	"fmt, (int)getpid(), __FUNCTION__, __LINE__, ##__VA_ARGS__)                                                                          
#else
    #define __DEBUG(fmt, ...)
#endif
#endif
