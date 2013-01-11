#ifndef _nessDB_SERIALIZE_H
#define _nessDB_SERIALIZE_H

#include "internal.h"
#include "buffer.h"
#include "xmalloc.h"

struct serial {
	struct buffer *buf;
};

struct serial *serial_new(int size);
void serialize(struct serial *s, struct sst_item *itms, int c);
struct sst_item *unserialize(struct serial *s, int c);
void serial_free(struct serial *s);

#endif
