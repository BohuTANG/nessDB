/*
 * copyright (c) 2012, bohutang <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "debug.h"

#define EVENT_NAME "ness.event"

void __debug_raw(int level, const char *msg, 
				 char *file, int line) 
{
	const char *c = ".-*#";
	time_t now = time(NULL);
	FILE *fp;
	char buf[64];

	strftime(buf, sizeof(buf), "%d %b %I:%M:%S", 
			localtime(&now));

	if (level == LEVEL_ERROR) {
		
		fp = fopen(EVENT_NAME, "a");
		if (fp) { 
			fprintf(stderr, "[%d] %s %c %s, os-error:%s %s:%d\n", 
					(int)getpid(), 
					buf, 
					c[level], 
					msg, 
					strerror(errno), 
					file, 
					line);
			fprintf(fp, "[%d] %s %c %s, os-error:%s %s:%d\n", 
					(int)getpid(),
					buf,
					c[level],
					msg,
					strerror(errno), 
					file, 
					line);
			fflush(fp);
			fclose(fp);
		}
	} else
		fprintf(stderr, "[%d] %s %c %s \n", 
				(int)getpid(), 
				buf, 
				c[level], 
				msg);
}

void __debug(char *file, int line, 
			 DEBUG_LEVEL level, const char *fmt, ...)
{
	va_list ap;
	char msg[1024];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	__debug_raw((int)level, msg, file, line);
}
