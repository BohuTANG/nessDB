/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "t.h"

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
 * msgpack is always extended to ALIGNMENT
 */
void _msgpack_extendby(struct msgpack *pk, uint32_t len)
{
	uint32_t new_size;

	len += pk->NUL;
	if (len == 0)
		len = ALIGNMENT;

	if (len <= pk->len)
		return;

	new_size = ALIGN(_next_power(len));
	pk->base = xrealloc_aligned(pk->base, pk->len, ALIGNMENT, new_size);
	pk->len = new_size;
}

static void putuint32(char *data, uint32_t val)
{
	data[0] = val & 0xff;
	data[1] = (val >> 8) & 0xff;
	data[2] = (val >> 16) & 0xff;
	data[3] = (val >> 24) & 0xff;
}

static void getuint32(char *data, uint32_t *v)
{
	*v = ((uint32_t)((unsigned char)data[0])
	      | ((uint32_t)((unsigned char)data[1]) << 8)
	      | ((uint32_t)((unsigned char)data[2]) << 16)
	      | ((uint32_t)((unsigned char)data[3]) << 24));
}

static void putuint64(char *data, uint64_t d)
{
	data[0] = d & 0xff;
	data[1] = (d >> 8) & 0xff;
	data[2] = (d >> 16) & 0xff;
	data[3] = (d >> 24) & 0xff;
	data[4] = (d >> 32) & 0xff;
	data[5] = (d >> 40) & 0xff;
	data[6] = (d >> 48) & 0xff;
	data[7] = (d >> 56) & 0xff;
}

static void getuint64(char *data, uint64_t *v)
{
	uint32_t hi, lo;

	getuint32(data, &lo);
	getuint32(data + 4, &hi);
	*v = ((uint64_t)(hi) << 32 | lo);
}

struct msgpack *msgpack_new(uint32_t size)
{
	struct msgpack *pk;

	pk = xcalloc(1, sizeof(*pk));
	_msgpack_extendby(pk, size);

	return pk;
}

int msgpack_clear(struct msgpack *pk)
{
	pk->NUL = 0;
	pk->SEEK = 0;
	return 1;
}

int msgpack_pack_nstr(struct msgpack *pk, const char *str, uint32_t n)
{
	_msgpack_extendby(pk, n + 1);
	memcpy(&pk->base[pk->NUL], str, n);
	pk->NUL += n;

	return 1;
}

int msgpack_unpack_nstr(struct msgpack *pk, uint32_t n, char **v)
{
	if (!n) return 1;

	if ((pk->SEEK + n) > pk->len)
		return 0;

	*v = (pk->base + pk->SEEK);
	pk->SEEK += n;

	return 1;
}

int msgpack_pack_uint8(struct msgpack *pk, uint8_t d)
{
	_msgpack_extendby(pk, 2);
	pk->base[pk->NUL++] = d;

	return 1;
}

int msgpack_unpack_uint8(struct msgpack *pk, uint8_t *v)
{
	if ((pk->SEEK + 1) > pk->len)
		return 0;

	*v = pk->base[pk->SEEK++];

	return 1;
}

int msgpack_pack_uint32(struct msgpack *pk, uint32_t d)
{
	_msgpack_extendby(pk, sizeof(uint32_t));
	putuint32(pk->base + pk->NUL, d);
	pk->NUL += sizeof(uint32_t);

	return 1;
}

int msgpack_unpack_uint32(struct msgpack *pk, uint32_t *v)
{
	*v = 0;
	if ((pk->SEEK + sizeof(uint32_t)) > pk->len)
		return 0;

	getuint32(pk->base + pk->SEEK, v);
	pk->SEEK += sizeof(uint32_t);

	return 1;
}

int msgpack_pack_uint64(struct msgpack *pk, uint64_t d)
{
	_msgpack_extendby(pk, sizeof(uint64_t));

	putuint64(pk->base + pk->NUL, d);
	pk->NUL += sizeof(uint64_t);

	return 1;
}

int msgpack_unpack_uint64(struct msgpack *pk, uint64_t *v)
{
	*v = 0UL;
	if ((pk->SEEK + sizeof(uint64_t)) > pk->len)
		return 0;

	getuint64(pk->base + pk->SEEK, v);
	pk->SEEK += sizeof(uint64_t);

	return 1;;
}

int msgpack_pack_msg(struct msgpack *pk, struct msg *d)
{
	msgpack_pack_uint32(pk, d->size);
	msgpack_pack_nstr(pk, d->data, d->size);

	return 1;
}

int msgpack_unpack_msg(struct msgpack *pk, struct msg *v)
{
	char *data;
	uint32_t size;

	if (!msgpack_unpack_uint32(pk, &size))
		return 0;
	if (!msgpack_unpack_nstr(pk, size, &data))
		return 0;

	v->size = size;
	v->data = xcalloc(1, size);
	memcpy(v->data, data, size);

	return 1;
}

int msgpack_pack_null(struct msgpack *pk, uint32_t n)
{
	uint32_t i;

	_msgpack_extendby(pk, n);
	for (i = 0; i < n; i++)
		msgpack_pack_uint8(pk, 0);

	return 1;
}

int msgpack_seek(struct msgpack *pk, uint32_t n)
{
	if (n > pk->len)
		return 0;

	pk->SEEK = n;

	return 1;
}

int msgpack_seekfirst(struct msgpack *pk)
{
	pk->SEEK = 0;
	return 1;
}

int msgpack_skip(struct msgpack *pk, uint32_t n)
{
	if ((pk->SEEK + n) > pk->len)
		return 0;

	pk->SEEK += n;

	return 1;
}

int msgpack_get_str(struct msgpack *pk, char **val)
{
	*val = (pk->base + pk->SEEK);

	return 1;
}

void msgpack_free(struct msgpack *pk)
{
	if (!pk) return;

	xfree(pk->base);
	xfree(pk);
}
