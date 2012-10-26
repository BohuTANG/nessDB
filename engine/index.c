/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/cola-algorithms.txt
 */

#include "config.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "index.h"
#include "debug.h"
#include "xmalloc.h"

struct index *index_new(const char *path)
{
	char db_name[NESSDB_PATH_SIZE];
	struct index *idx = xcalloc(1, sizeof(struct index));

	idx->meta = meta_new(path);
	idx->buf = buffer_new(5 * 1024 * 1024);

	memset(db_name, 0, NESSDB_PATH_SIZE);
	snprintf(db_name, NESSDB_PATH_SIZE, "%s/ness.db", path);

	idx->fd = n_open(db_name, N_OPEN_FLAGS, 0644);
	if (idx->fd == -1) {
		idx->fd = n_open(db_name, N_CREAT_FLAGS, 0644);
		if (idx->fd == -1) 
			__PANIC("db error, name#%s", db_name);
	}

	idx->read_fd = n_open(db_name, N_OPEN_FLAGS);

	idx->db_alloc = n_lseek(idx->fd, 0, SEEK_END);

	return idx;
}

int index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	int ret;
	int len;
	char *line;
	struct meta_node *node ;

	/* write value */
	buffer_putint(idx->buf, sv->len);
	buffer_putnstr(idx->buf, sv->data, sv->len);
	len = idx->buf->NUL;
	line = buffer_detach(idx->buf);
	ret = write(idx->fd, line, len);
	if (ret == -1) 
		__PANIC("write key error");

	/* write key index */
	node = meta_get(idx->meta, sk->data);
	cola_add(node->cola, sk, idx->db_alloc, 1);

	/* update db alloc */
	idx->db_alloc += len;

	return 1;
}

int index_get(struct index *idx, struct slice *sk, struct slice *sv) 
{
	int res;
	uint64_t off = 0UL;
	struct meta_node *node = meta_get(idx->meta, sk->data);

	if (node) 
		off = cola_get(node->cola, sk);

	if (off > 0) {
		int vlen = 0;
		char *data;

		n_lseek(idx->read_fd, off, SEEK_SET);
		res = pread(idx->read_fd, &vlen, sizeof(int), off);
		if (res == -1) {
			__ERROR("get vlen error, key#%d", sk->data);
			goto RET;
		}

		data = xcalloc(1, vlen + 1);

		res = pread(idx->read_fd, data, vlen, off + sizeof(int));
		if (res == -1) {
			__ERROR("get data error, key#%d", sk->data);
			goto RET;
		}

		sv->data = data;
		sv->len = vlen;

		return 1;
	}

RET:
	return 0;
}

int index_remove(struct index *idx, struct slice *sk)
{
	struct meta_node *node ;

	/* write key index */
	node = meta_get(idx->meta, sk->data);
	cola_add(node->cola, sk, 0UL, 0);

	return 1;
}

void index_free(struct index *idx)
{
	meta_free(idx->meta);
	free(idx);
}
