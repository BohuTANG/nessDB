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
