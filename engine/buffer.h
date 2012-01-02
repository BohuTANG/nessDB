/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

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
void buffer_putint(struct buffer *b, int val);
uint32_t u32_from_big(unsigned char *buf);
void buffer_putlong(struct buffer *b, uint64_t val);
uint64_t u64_from_big(unsigned char *buf);

void buffer_dump(struct buffer *b);

#endif
