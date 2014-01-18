/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"

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

/*
 * buffer is always extended to ALIGNMENT
 */
void _buf_extendby(struct buffer *b, uint32_t len)
{
	uint32_t new_size;

	len += b->NUL;
	if (len == 0)
		len = ALIGNMENT;

	if (len <= b->buflen)
		return;

	new_size = ALIGN(_next_power(len));
	b->buf = xrealloc_aligned(b->buf, b->buflen, ALIGNMENT, new_size);
	b->buflen = new_size;
}

struct buffer *buf_new(uint32_t size)
{
	struct buffer *b;

	b = xcalloc(1, sizeof(*b));
	_buf_extendby(b, size);

	return b;
}

void buf_clear(struct buffer *b)
{
	b->NUL = 0;
	b->SEEK = 0;

	if (b->buf)
		b->buf[b->NUL] = '\0';
}

void putnstr(char *base, char *src, uint32_t n)
{
	memcpy(base, src, n);
}

void buf_putnstr(struct buffer *b, const char *str, uint32_t n)
{
	_buf_extendby(b, n + 1);
	memcpy(&b->buf[b->NUL], str, n);
	b->NUL += n;
	b->buf[b->NUL] = '\0';
}

void getnstr(char *base, uint32_t n, char **val)
{
	*val = (base + n);
}

int buf_getnstr(struct buffer *b, uint32_t n, char **val)
{
	if (!n) return 1;

	if ((b->SEEK + n) > b->buflen)
		return 0;

	*val = (b->buf + b->SEEK);
	b->SEEK += n;

	return 1;
}

void buf_putc(struct buffer *b, const char c)
{
	_buf_extendby(b, 2);
	b->buf[b->NUL++] = c;
	b->buf[b->NUL] = '\0';
}

int buf_getc(struct buffer *b, char *val)
{
	if ((b->SEEK + 1) > b->buflen)
		return 0;

	*val = b->buf[b->SEEK++];

	return 1;
}

void putuint32(char *data, uint32_t val)
{
	data[0] = val & 0xff;
	data[1] = (val >> 8) & 0xff;
	data[2] = (val >> 16) & 0xff;
	data[3] = (val >> 24) & 0xff;
}

void getuint32(char *data, uint32_t *val)
{
	*val = ((uint32_t)((unsigned char)data[0])
			| ((uint32_t)((unsigned char)data[1]) << 8)
			| ((uint32_t)((unsigned char)data[2]) << 16)
			| ((uint32_t)((unsigned char)data[3]) << 24));
}

void putuint64(char *data, uint64_t val)
{
	data[0] = val & 0xff;
	data[1] = (val >> 8) & 0xff;
	data[2] = (val >> 16) & 0xff;
	data[3] = (val >> 24) & 0xff;
	data[4] = (val >> 32) & 0xff;
	data[5] = (val >> 40) & 0xff;
	data[6] = (val >> 48) & 0xff;
	data[7] = (val >> 56) & 0xff;
}

void getuint64(char *data, uint64_t *val)
{
	uint32_t hi, lo;

	getuint32(data, &lo);
	getuint32(data + 4, &hi);
	*val = ((uint64_t)(hi) << 32 | lo);
}

void buf_putuint32(struct buffer *b, uint32_t val)
{
	_buf_extendby(b, sizeof(int));
	putuint32(b->buf + b->NUL, val);
	b->NUL += sizeof(uint32_t);
}

int buf_getuint32(struct buffer *b, uint32_t *val)
{
	*val = 0;
	if ((b->SEEK + 4) > b->buflen)
		return 0;

	getuint32(b->buf + b->SEEK, val);
	b->SEEK += sizeof(uint32_t);

	return 1;
}

void buf_putuint64(struct buffer *b, uint64_t val)
{
	_buf_extendby(b, sizeof(uint64_t));

	putuint64(b->buf + b->NUL, val);
	b->NUL += sizeof(uint64_t);
}

int buf_getuint64(struct buffer *b, uint64_t *val)
{
	*val = 0UL;
	if ((b->SEEK + 8) > b->buflen)
		return 0;

	getuint64(b->buf + b->SEEK, val);
	b->SEEK += sizeof(uint64_t);

	return 1;;
}

void buf_putnull(struct buffer *b, uint32_t n)
{
	uint32_t i;

	_buf_extendby(b, n);
	for (i = 0; i < n; i++)
		buf_putc(b, 0);
}

int buf_seek(struct buffer *b, uint32_t n)
{
	if (n > b->buflen)
		return 0;

	b->SEEK = n;
	return 1;
}


void buf_seekfirst(struct buffer *b)
{
	b->SEEK = 0;
}

int buf_skip(struct buffer *b, uint32_t n)
{
	if ((b->SEEK + n) > b->buflen)
		return 0;

	b->SEEK += n;
	return 1;
}

void buf_pos(struct buffer *b, char **val)
{
	*val = (b->buf + b->SEEK);
}

int buf_xsum(const char *data, uint32_t len, uint32_t *xsum)
{
	if (len == 0)
		return 0;

	*xsum = crc32(data, len);

	return 1;
}

void buf_putmsg(struct buffer *b, struct msg *msg)
{
	buf_putuint32(b, msg->size);
	buf_putnstr(b, msg->data, msg->size);
}

int buf_getmsg(struct buffer *b, struct msg *val)
{
	char *data;
	uint32_t size;

	if (!buf_getuint32(b, &size))
		return 0;
	if (!buf_getnstr(b, size, &data))
		return 0;

	val->size = size;
	val->data = xcalloc(1, size);
	memcpy(val->data, data, size);

	return 1;
}

void buf_free(struct buffer *b)
{
	if (!b) return;

	xfree(b->buf);
	xfree(b);
}
