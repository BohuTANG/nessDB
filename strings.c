#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "strings.h"

struct strings {
    char *buf;
    int pos;
    int buflen;
};

static unsigned
next_power(unsigned x)
{
	--x;
	x |= x >> 0x01;
	x |= x >> 0x02;
	x |= x >> 0x04;
	x |= x >> 0x08;
	x |= x >> 0x10;
	return ++x;
}

/* Extend the buffer in sb by at least len bytes.
 * Note len should include the space required for the NUL terminator */
static void
string_extendby(struct strings *sb, int len)
{
	char *buf;

	len += sb->pos;
	if (len <= sb->buflen)
		return;

	if (!sb->buflen)
		sb->buflen = 32;
	else
		sb->buflen = next_power(len);

	buf = (char*)realloc(sb->buf, sb->buflen);
	sb->buf = buf;
}

static void
string_vprintf(struct strings *sb, const char *fmt, const va_list ap)
{
	int num_required;
	while ((num_required = vsnprintf(sb->buf + sb->pos, sb->buflen - sb->pos, fmt, ap)) >= sb->buflen - sb->pos)
		string_extendby(sb, num_required + 1);
	sb->pos += num_required;
}

////////////////////////////////////////////////////////////////////////////////

struct strings *
string_new(size_t reserve)
{
	struct strings *sb = (struct strings*)malloc(sizeof(struct strings));
	sb->buf = NULL;
	sb->pos = 0;
	sb->buflen = 0;

	if (reserve)
		string_extendby(sb, reserve + 1);

	return sb;
}

void
string_free(struct strings *sb)
{
	if (sb->buf)
		free(sb->buf);
	free(sb);
}

/* after detach must free(buf)
 * or memory leak*/
char *
string_detach(struct strings *sb)
{
	char *buf = sb->buf;
	sb->buf = NULL;
	sb->pos = 0;
	sb->buflen = 0;
	return buf;
}

void
string_clear(struct strings *sb)
{
	sb->pos = 0;
	if (sb->buf)
		sb->buf[sb->pos] = '\0';
}

void
string_cat(struct strings *sb, const char *str)
{
	size_t len = strlen(str);

	string_extendby(sb, len + 1);
	memcpy(&sb->buf[sb->pos], str, len);
	sb->pos += len;
	sb->buf[sb->pos] = '\0';
}

void
string_putc(struct strings *sb, const char c)
{
	string_extendby(sb, 2);
	sb->buf[sb->pos++] = c;
	sb->buf[sb->pos] = '\0';
}

void
string_ncat(struct strings *sb, const char *str, size_t len)
{
	string_extendby(sb, len + 1);
	memcpy(&sb->buf[sb->pos], str, len);
	sb->pos += len;
	sb->buf[sb->pos] = '\0';
}

void
string_scatf(struct strings *sb, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	string_vprintf(sb, fmt, ap);
	va_end(ap);
}

const char *
string_raw(struct strings *sb)
{
	return sb->pos ? sb->buf : NULL;
}

size_t
string_len(struct strings *sb)
{
	return sb->pos;
}

