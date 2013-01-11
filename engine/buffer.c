/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "xmalloc.h"
#include "buffer.h" 
#include "debug.h"

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
	buffer = xrealloc(b->buf, b->buflen);
	b->buf = buffer;
}

void _string_vprintf(struct buffer *b,
					 const char *fmt, va_list ap)
{
	int num_required;
	
	while ((num_required = vsnprintf(b->buf + b->NUL, b->buflen - b->NUL, fmt, ap)) >= b->buflen - b->NUL)
		_buffer_extendby(b, num_required + 1);

	b->NUL += num_required;
}

struct buffer *buffer_new(size_t reserve)
{
	struct buffer *b = xmalloc(sizeof(struct buffer));

	memset(b, 0, sizeof(struct buffer));
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

void buffer_putint(struct buffer *b, int val)
{
	_buffer_extendby(b, sizeof(int));
	b->buf[b->NUL++] = val & 0xff;
	b->buf[b->NUL++] = (val >> 8) & 0xff;
	b->buf[b->NUL++] = (val >> 16) & 0xff;
	b->buf[b->NUL++] = (val >> 24) & 0xff;
}

void buffer_putshort(struct buffer *b, short val)
{
	_buffer_extendby(b, sizeof(short));
	b->buf[b->NUL++] = val & 0xff;
	b->buf[b->NUL++] = (val >> 8) & 0xff;
}

void buffer_putlong(struct buffer *b, uint64_t val)
{
	_buffer_extendby(b, sizeof(uint64_t));
	b->buf[b->NUL++] = val & 0xff;
	b->buf[b->NUL++] = (val >> 8) & 0xff;
	b->buf[b->NUL++] = (val >> 16) & 0xff;
	b->buf[b->NUL++] = (val >> 24) & 0xff;
	b->buf[b->NUL++] = (val >> 32) & 0xff;
	b->buf[b->NUL++] = (val >> 40) & 0xff;
	b->buf[b->NUL++] = (val >> 48) & 0xff;
	b->buf[b->NUL++] = (val >> 56) & 0xff;
}

void buffer_scatf(struct buffer *b, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	_string_vprintf(b, fmt, ap);
	va_end(ap);
}

char * buffer_detach(struct buffer *b)
{
	char *buffer = b->buf;

	b->NUL = 0;
	return buffer;
}

void buffer_seekfirst(struct buffer *b)
{
	b->SEEK = 0;
}

char buffer_getchar(struct buffer *b)
{
	return b->buf[b->SEEK++];
}

uint32_t buffer_getint(struct buffer *b)
{
	uint32_t val = 0;

	val |= b->buf[b->SEEK++];
	val |= b->buf[b->SEEK++] << 8;
	val |= b->buf[b->SEEK++] << 16;
	val |= b->buf[b->SEEK++] << 24;

	return val;
}

uint64_t buffer_getlong(struct buffer *b) 
{
	uint64_t val = 0;

	val |= b->buf[b->SEEK++];
	val |= b->buf[b->SEEK++] << 8;
	val |= b->buf[b->SEEK++] << 16;
	val |= b->buf[b->SEEK++] << 24;
	val |= (uint64_t)b->buf[b->SEEK++] << 32;
	val |= (uint64_t)b->buf[b->SEEK++] << 40;
	val |= (uint64_t)b->buf[b->SEEK++] << 48;
	val |= (uint64_t)b->buf[b->SEEK++] << 56;

	return val;
}

char *buffer_getnstr(struct buffer *b, size_t n)
{
	char *str;

	str = (b->buf + b->SEEK);
	b->SEEK += n;

	return str;
}

void buffer_dump(struct buffer *b)
{
	int i;

	printf("--buffer dump:buflen<%d>,pos<%d>\n", b->buflen, b->NUL);

	for (i = 0; i < b->NUL; i++)
		printf("\t[%d] %c\n", i, b->buf[i]);
}
