/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

#define EVENT_NAME "ness.event"

void __debug_raw(int level,
                 const char *msg,
                 char *file,
                 int line)
{
	char buf[64];
	time_t now = time(NULL);
	FILE *fp = fopen(EVENT_NAME, "a");
	const char *c[] = {"INFO", "DEBUG", "WARN", "ERROR"};

	if (fp) {
		strftime(buf, sizeof(buf), "%d %b %I:%M:%S", localtime(&now));
		fprintf(fp, "[%d] | %s | %s | %s:%d | %s\n", (int)getpid(), buf, c[level], file, line, msg);

		fflush(fp);
		fclose(fp);
	}
}

void __debug(char *file, int line,
             enum DEBUG_LEVEL level,
             const char *fmt, ...)
{
	va_list ap;
	char msg[1024];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	__debug_raw((int)level, msg, file, line);
}

void _assert(void *expr, void *filename, unsigned lineno)
{
	printf("Assertion failed: %s, file %s, line %d\n", (char*)expr, (char*)filename, lineno);
	exit(3);
}
