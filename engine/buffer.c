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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer.h"

unsigned _next_power(unsigned x)
{
	--x;
	x |= x >> 0x01;
	x |= x >> 0x02;
	x |= x >> 0x04;
	x |= x >> 0x08;
	x |= x >> 0x10;
	return ++x;
}

void _buffer_extendby(struct buffer *b, int len)
{
	char* buffer;

	len += b->NUL;
	if (len <= b->buflen)
		return;

	if (!b->buflen)
		b->buflen = 32;

	b->buflen = _next_power(len);
	buffer = realloc(b->buf, b->buflen);
	b->buf = buffer;
}


struct buffer *buffer_new(size_t reserve)
{
	struct buffer *b = malloc(sizeof(struct buffer));
	b->buf = NULL;
	b->NUL = 0;
	b->buflen = 0;

	if (reserve)
		_buffer_extendby(b, reserve + 1);

	return b;
}

void buffer_free(struct buffer *b)
{
	if (b->buf)
		free(b->buf);
	free(b);
}

void buffer_clear(struct buffer *b)
{
	b->NUL = 0;
	if (b->buf)
		b->buf[b->NUL] = '\0';
}

void buffer_putstr(struct buffer *b, const char *str)
{
	size_t len = strlen(str);

	_buffer_extendby(b, len + 1);
	memcpy(&b->buf[b->NUL], str, len);
	b->NUL += len;
	b->buf[b->NUL] = '\0';
}

void buffer_putnstr(struct buffer *b, const char *str,size_t n)
{
	_buffer_extendby(b, n + 1);
	memcpy(&b->buf[b->NUL], str, n);
	b->NUL += n;
	b->buf[b->NUL] = '\0';
}

void buffer_putc(struct buffer *b, const char c)
{
	_buffer_extendby(b, 2);
	b->buf[b->NUL++] = c;
	b->buf[b->NUL] = '\0';
}

void buffer_putint(struct buffer *b, int i)
{
	unsigned int value = i;
	_buffer_extendby(b, sizeof(int));
	b->buf[b->NUL++] = (char)(value);
	b->buf[b->NUL++] = (char)(value >> 0x08);
	b->buf[b->NUL++] = (char)(value >> 0x10);
	b->buf[b->NUL++] = (char)(value >> 0x18);
}

char * buffer_detach(struct buffer *b)
{
	char *buffer = b->buf;
	b->NUL = 0;
	b->buflen = 0;
	return buffer;
}

void buffer_dump(struct buffer *b)
{
	int i;
	printf("--buffer dump:buflen<%d>,pos<%d>\n", b->buflen, b->NUL);

	for (i = 0; i < b->NUL; i++)
		printf("	[%d] %c\n", i, b->buf[i]);
}
