 /* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of lsmtree nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __USE_FILE_OFFSET64
	#define __USE_FILE_OFFSET64
#endif

#ifndef __USE_LARGEFILE64
	#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE64_SOURCE
	#define _LARGEFILE64_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "buffer.h"
#include "log.h"
#include "platform.h"
#include "debug.h"

struct log *log_new(char *name)
{
	struct log *l;
	char log_name[LOG_NSIZE];
	int fd;

	l = malloc(sizeof(struct log));

	memset(log_name, 0 ,LOG_NSIZE);
	snprintf(log_name, LOG_NSIZE, "%s.log", name);
	memcpy(l->name, log_name, LOG_NSIZE);

	fd = open(log_name, BTREE_OPEN_FLAGS, 0644);
	if (fd > -1) {
		l->fd = fd;
		__DEBUG("%s", "WARNING: Find log file,need to recover");
		/*TODO: log recover*/
	} else
		l->fd = open(log_name, BTREE_CREAT_FLAGS, 0644);

	l->buf = buffer_new(1024*1024);

	return l;
}

void log_append(struct log *l, struct slice *sk, uint64_t offset, uint8_t opt)
{
	char *line;
	int len;
	struct buffer *buf = l->buf;

	buffer_putc(buf, (char)opt);
	buffer_putint(buf, sk->len);
	buffer_putnstr(buf, sk->data, sk->len);

	/* If opt is not 1, it maybe remove*/
	if(opt == 1)
		buffer_putlong(buf, offset);

	len = buf->NUL;
	line = buffer_detach(buf);
	if (write(l->fd, line, len) != len)
		__DEBUG("%s,buffer is:%s,buffer length:<%d>", "ERROR: Log AOF **ERROR**", line, len);
}

void log_trunc(struct log *l)
{
	buffer_clear(l->buf);
	remove(l->name);
	l->fd = open(l->name, BTREE_CREAT_FLAGS, 0644);
}

void log_free(struct log *l)
{
	if (l) {
		buffer_free(l->buf);
		remove(l->name);
		close(l->fd);
		free(l);
	}
}
