#ifndef __nessDB_BUF_H
#define __nessDB_BUF_H

#include <stdint.h>

struct buffer {
    char *buf;
	int SEEK;
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
void buffer_putint(struct buffer *b, int val);
void buffer_scatf(struct buffer *b, const char *fmt, ...);
void buffer_putlong(struct buffer *b, uint64_t val);
void buffer_putshort(struct buffer *b, short val);

uint32_t buffer_getint(struct buffer *b);
uint64_t buffer_getlong(struct buffer *b);
char *buffer_getnstr(struct buffer *b, size_t n);
char buffer_getchar(struct buffer *b);
void buffer_seekfirst(struct buffer *b);

void buffer_dump(struct buffer *b);

#endif
