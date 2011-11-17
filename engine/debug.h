#ifndef _DEBUG_H_
#define _DEBUG_H_

#include <stdarg.h>
#include <stdio.h>
#include <time.h>

void _debug_raw(const char *msg) {
   
    time_t now = time(NULL);
    FILE *fp;
    char buf[64];

    fp = fopen("log.txt","a");
    if (!fp) return;

    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",localtime(&now));
    fprintf(fp,"[%d] %s  %s\n",(int)getpid(),buf,msg);
    fflush(fp);
    fclose(fp);
}


void _debug(const char *fmt, ...) {
    va_list ap;
    char msg[1024];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
     
    _debug_raw(msg);
}

#endif
