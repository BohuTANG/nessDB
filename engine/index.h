#ifndef _INDEX_H
#define _INDEX_H

#include "meta.h"
#include "buffer.h"

struct index{
	int fd;
	int read_fd;
	uint64_t db_alloc;
	struct meta *meta;
	struct buffer *buf;
};

struct index *index_new(const char *path);
int index_add(struct index *idx, struct slice *sk, struct slice *sv);
int index_get(struct index *idx, struct slice *sk, struct slice *sv);
int index_remove(struct index *idx, struct slice *sk);
void index_free(struct index *idx);

#endif
