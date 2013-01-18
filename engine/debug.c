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


static void *__get_context_eip(ucontext_t *uc)
{
#if defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
    /* OSX < 10.6 */
    #if defined(__x86_64__)
    return (void *) uc->uc_mcontext->__ss.__rip;
    #elif defined(__i386__)
    return (void *) uc->uc_mcontext->__ss.__eip;
    #else
    return (void *) uc->uc_mcontext->__ss.__srr0;
    #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
    /* OSX >= 10.6 */
    #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void *) uc->uc_mcontext->__ss.__rip;
    #else
    return (void *) uc->uc_mcontext->__ss.__eip;
    #endif
#elif defined(__linux__)
    /* Linux */
    #if defined(__i386__)
    return (void *) uc->uc_mcontext.gregs[14]; /* Linux 32 */
    #elif defined(__X86_64__) || defined(__x86_64__)
    return (void *) uc->uc_mcontext.gregs[16]; /* Linux 64 */
    #elif defined(__ia64__) /* Linux IA64 */
    return (void *) uc->uc_mcontext.sc_ip;
    #endif
#else
    return NULL;
#endif
}

void __log_stacktrace(ucontext_t *uc)
{
	void *trace[100];
	int trace_size = 0, fd;

	fd = open(EVENT_NAME, O_APPEND | O_CREAT | O_WRONLY, 0644);
	if (fd == -1) 
		return;

	/* Generate the stack trace */
	trace_size = backtrace(trace, 100);

	/* overwrite sigaction with caller's address */
	if (__get_context_eip(uc) != NULL)
		trace[1] = __get_context_eip(uc);

	/* Write symbols to log file */
	backtrace_symbols_fd(trace, trace_size, fd);
	close(fd);
	exit(EXIT_FAILURE);
}

void __dog_signal(int sig, siginfo_t *info, void *secret)
{
	(void)sig;
	(void)info;

    ucontext_t *uc = (ucontext_t *) secret;
    __log_stacktrace(uc);
}

void __DEBUG_INIT_SIGNAL() 
{
	struct sigaction act;

	/* Watchdog was actually disabled, so we have to setup the signal
	 * handler. */
	sigemptyset(&act.sa_mask);
	act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_SIGINFO;
	act.sa_sigaction = __dog_signal;
	sigaction(SIGSEGV, &act, NULL);
}
