#ifndef _BUF_H
#define _BUF_H

#include <stdint.h>

struct buffer {
    char *buf;
	int NUL;
    int buflen;
};

struct buffer *buffer_new(size_t reserve);
void buffer_free(struct buffer *b);

void buffer_clear(struct buffer *b);
char *buffer_detach(struct buffer *b);

void buffer_putc(struct buffer *b, const char c);
void buffer_putstr(struct buffer *b, const char *str);
void buffer_putnstr(struct buffer *b, const char *str, size_t n);
void buffer_putint(struct buffer *b, uint32_t val);
void buffer_putlong(struct buffer *b, uint64_t val);

void buffer_dump(struct buffer *b);

#endif
