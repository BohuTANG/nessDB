/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_BUF_H_
#define nessDB_BUF_H_

#include "internal.h"
#include "crc32.h"
#include "msg.h"

/**
 * a buffer stream
 */

struct buffer {
	char *buf;
	uint32_t NUL;
	uint32_t SEEK;
	uint32_t buflen;
};

struct buffer *buf_new(uint32_t size);
void buf_free(struct buffer *b);
void buf_putc(struct buffer *b, const char c);
int buf_getc(struct buffer *b, char *c);

void putnstr(char *base, char *src, uint32_t n);
void getnstr(char *base, uint32_t n, char **val);
void buf_putnstr(struct buffer *b, const char *str, uint32_t n);
int buf_getnstr(struct buffer *b, uint32_t n, char **val);

void putuint32(char *data, uint32_t val);
void getuint32(char *data, uint32_t *val);
void buf_putuint32(struct buffer *b, uint32_t val);
int buf_getuint32(struct buffer *b, uint32_t *val);

void buf_putuint64(struct buffer *b, uint64_t val);
int buf_getuint64(struct buffer *b, uint64_t *val);

void buf_putnull(struct buffer *b, uint32_t n);
int buf_seek(struct buffer *b, uint32_t s);
void buf_seekfirst(struct buffer *b);
int buf_skip(struct buffer *b, uint32_t n);
void buf_pos(struct buffer *b, char **val);
void buf_clear(struct buffer *b);
void buf_putmsg(struct buffer *b, struct msg *val);
int buf_getmsg(struct buffer *b, struct msg *val);
int buf_xsum(const char *data, uint32_t len, uint32_t *xsum);

#endif /* nessDB_BUF_H_ */
