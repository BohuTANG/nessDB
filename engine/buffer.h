#ifndef _BUF_H
#define _BUF_H

#include <stdint.h>

struct buffer {
    char *buf;
	int NUL;
	int POS;
    int buflen;
};

struct buffer *buffer_new(size_t reserve);
void buffer_free(struct buffer *b);

void buffer_clear(struct buffer *b);
char *buffer_detach(struct buffer *b);

void buffer_putc(struct buffer *b, const char c);
char buffer_getc(struct buffer *b);
void buffer_putstr(struct buffer *b, const char *str);
void buffer_putnstr(struct buffer *b, const char *str, size_t n);
void buffer_getnstr(struct buffer *b, char *str, size_t n);
void buffer_putint(struct buffer *b, int val);
uint32_t buffer_getuint(struct buffer *b);
void buffer_scatf(struct buffer *b, const char *fmt, ...);
uint64_t buffer_getulong(struct buffer *b);
void buffer_putlong(struct buffer *b, uint64_t val);
void buffer_putshort(struct buffer *b, short val);

void buffer_dump(struct buffer *b);

#endif
