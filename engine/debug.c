#include <stdio.h>
#include <time.h>
#include <stdarg.h>
#include "debug.h"

#define EVENT_NAME "ness.event"

void __debug_raw(int level, const char *msg) 
{
	const char *c = ".-*#";
	time_t now = time(NULL);
	FILE *fp;
	char buf[64];

	strftime(buf, sizeof(buf),"%d %b %I:%M:%S", localtime(&now));
	fprintf(stderr, "[%d] %s %c %s\n", (int)getpid(), buf, c[level], msg);

	if (level == LEVEL_ERROR || level == LEVEL_DEBUG) {
		fp = fopen(EVENT_NAME, "a");
		if (fp) { 
			fprintf(fp,"[%d] %s %c %s\n", (int)getpid(), buf, c[level], msg);
			fflush(fp);
			fclose(fp);
		}
	}
}

void __DEBUG(DEBUG_LEVEL level, const char *fmt, ...) 
{
	va_list ap;
	char msg[1024];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	__debug_raw((int)level,msg);
}
