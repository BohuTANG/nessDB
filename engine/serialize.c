/*
 * Copyright (c) 2012-2013, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "serialize.h"

struct serial *serial_new(int size)
{
	struct serial *s = xcalloc(1, sizeof(struct serial));

	s->buf = buffer_new(size);

	return s;
}

void serialize(struct serial *s, struct sst_item *itms, int c)
{
	int i;
	int klen;
	int psize;
	struct sst_item itm;

	buffer_clear(s->buf);

	for (i = 0; i < c; i++) {

		itm = itms[i];
		klen = strlen(itm.data);
		psize = klen + sizeof(struct sst_item) - NESSDB_MAX_KEY_SIZE;

		buffer_putint(s->buf, psize);
		buffer_putint(s->buf, klen);
		buffer_putnstr(s->buf, itm.data, klen);
		buffer_putlong(s->buf, itm.offset);
		buffer_putint(s->buf, itm.vlen);
		buffer_putc(s->buf, itm.opt);
	}
}

struct sst_item *unserialize(struct serial *s, int c)
{
	int i;
	int klen;
	struct sst_item *itms;

	itms = xcalloc(c + 1, sizeof(struct sst_item));

	for (i = 0; i < c; i++) {
		buffer_getint(s->buf);
		klen = buffer_getint(s->buf);
		memcpy(itms[i].data, buffer_getnstr(s->buf, klen), klen);
		itms[i].offset = buffer_getlong(s->buf);
		itms[i].vlen = buffer_getint(s->buf);
		itms[i].opt = buffer_getchar(s->buf);
	}

	return  itms;
}

void serial_free(struct serial *s)
{
	if (s->buf)
		buffer_free(s->buf);
	xfree(s);
}
